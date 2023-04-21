package surfstore

import (
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	reflect "reflect"
)

func checkErr(err error) {
	if err != nil {
		log.Panicln(err)
	}
}

func UpdateRemoteServer(client RPCClient, indexFileMetaData *FileMetaData) int32 {
	err := client.UpdateFile(indexFileMetaData, &indexFileMetaData.Version)
	checkErr(err)
	// The file is in the FileMetaMap but the version is WRONG, set version to -1(upload failed)
	if indexFileMetaData.GetVersion() != -1 {
		// deletion, no need to update blocks content
		if len(indexFileMetaData.GetBlockHashList()) == 1 && indexFileMetaData.GetBlockHashList()[0] == "0" {
			return indexFileMetaData.GetVersion()
		}
		// update each block's content in the server
		// either insertion or modification
		fileName := indexFileMetaData.GetFilename()
		filePath := ConcatPath(client.BaseDir, fileName)
		file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
		checkErr(err)
		defer file.Close()

		fileInfo, err := os.Stat(filePath)
		checkErr(err)

		serverFileInfoMap := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&serverFileInfoMap)
		checkErr(err)
		fileBlockHashes := serverFileInfoMap[fileName].GetBlockHashList()
		blockStoreMap := make(map[string][]string)
		client.GetBlockStoreMap(fileBlockHashes, &blockStoreMap)

		hashBlockStoreMap := make(map[string]string) // blockHashValue : block store addr
		getHashBlockStoreMap(&blockStoreMap, &hashBlockStoreMap)

		numberOfBlock := int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))
		lastBlockSize := fileInfo.Size() % int64(client.BlockSize)
		for i := 0; i < numberOfBlock-1; i++ {
			var block Block
			block.BlockData = make([]byte, client.BlockSize)
			n, err := io.ReadFull(file, block.BlockData)
			if err != nil {
				if err == io.EOF {
					break
				}
				checkErr(err)
			}
			block.BlockSize = int32(n)
			block.BlockData = block.BlockData[:n]

			// var blockStoreAddr string
			// err = client.GetBlockStoreAddr(&blockStoreAddr)
			// checkErr(err)

			blockStoreAddr := hashBlockStoreMap[fileBlockHashes[i]]

			var success bool
			err = client.PutBlock(&block, blockStoreAddr, &success)
			checkErr(err)
		}
		// the last block
		var block Block
		block.BlockData = make([]byte, lastBlockSize)
		n, err := io.ReadFull(file, block.BlockData)
		if err != nil {
			if err != io.EOF {
				checkErr(err)
			}
		}
		if n > 0 {
			block.BlockSize = int32(n)
			block.BlockData = block.BlockData[:n]

			// var blockStoreAddr string
			// err = client.GetBlockStoreAddr(&blockStoreAddr)
			// checkErr(err)

			blockStoreAddr := hashBlockStoreMap[fileBlockHashes[numberOfBlock-1]]

			var success bool
			err = client.PutBlock(&block, blockStoreAddr, &success)
			checkErr(err)
		}
	}
	return indexFileMetaData.GetVersion()
}

// return a map whose key is block hash value and value is block store addr(server)
func getHashBlockStoreMap(blockStoreMap *map[string][]string,
	hashBlockStoreMap *map[string]string) {

	for serverName, blockHashes := range *blockStoreMap {
		for _, blockHash := range blockHashes {
			(*hashBlockStoreMap)[blockHash] = serverName
		}
	}
}

func DownloadLatestVersion(client RPCClient, remoteFileMetaData *FileMetaData, indexFileInfoMap *map[string]*FileMetaData) {
	filePath := ConcatPath(client.BaseDir, remoteFileMetaData.GetFilename())

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	checkErr(err)
	defer file.Close()

	remoteFileName := remoteFileMetaData.GetFilename()
	// If the server delete the file, remove it in the base directory and update the index.db
	if len(remoteFileMetaData.GetBlockHashList()) == 1 && remoteFileMetaData.GetBlockHashList()[0] == "0" {
		// Update the indexFileInfoMap
		(*indexFileInfoMap)[remoteFileName].Version = remoteFileMetaData.GetVersion()
		(*indexFileInfoMap)[remoteFileName].BlockHashList = remoteFileMetaData.GetBlockHashList()
		// Delete the file in the base directory.
		err := os.Remove(filePath)
		checkErr(err)
		return
	}

	// update the latest file in the base directory and update the index.db
	// 1. a file existed in the server that wasn’t locally present
	// 1.1 create new metadata and put it into indexFileInfoMap
	// 1.2 create a new file in the base directory
	// 2. the file which exists in the index.db is not the latest version
	// 2.1 update metadata in the indexFileInfoMap
	// 2.2 create a new file in the base directory

	// handle meta data in indexFileInfoMap
	_, indexFileOk := (*indexFileInfoMap)[remoteFileName]
	if !indexFileOk {
		newIndexFileMetaData := FileMetaData{
			Filename:      remoteFileName,
			Version:       remoteFileMetaData.GetVersion(),
			BlockHashList: remoteFileMetaData.GetBlockHashList(),
		}
		(*indexFileInfoMap)[remoteFileName] = &newIndexFileMetaData
	} else {
		// Update the indexFileInfoMap
		(*indexFileInfoMap)[remoteFileName].Version = remoteFileMetaData.GetVersion()
		(*indexFileInfoMap)[remoteFileName].BlockHashList = remoteFileMetaData.GetBlockHashList()
	}

	// create a new file in the base directory
	var block Block
	blockHashList := remoteFileMetaData.GetBlockHashList()
	blockStoreMap := make(map[string][]string)
	client.GetBlockStoreMap(blockHashList, &blockStoreMap)
	hashBlockStoreMap := make(map[string]string) // blockHashValue : block store addr
	getHashBlockStoreMap(&blockStoreMap, &hashBlockStoreMap)

	for _, hashVal := range blockHashList {
		// var blockStoreAddr string
		// err := client.GetBlockStoreAddr(&blockStoreAddr)
		// checkErr(err)
		blockStoreAddr := hashBlockStoreMap[hashVal]
		err = client.GetBlock(hashVal, blockStoreAddr, &block)
		// if err != nil {
		// 	log.Println("Fail to get block store address ", err)
		// }
		checkErr(err)

		blockData := block.GetBlockData()
		_, err = file.Write(blockData)
		checkErr(err)

	}

}

func LocalRemoteSync(baseDirFileInfoMap *map[string]*FileMetaData, indexFileInfoMap *map[string]*FileMetaData, remoteFileInfoMap *map[string]*FileMetaData, client RPCClient) {
	var returnVersion int32
	returnVersion = -1
	// sync each file in index.db to remote server
	for indexFileName, indexFileMetaData := range *indexFileInfoMap {
		remoteFileMetaData, remoteIndexFileOk := (*remoteFileInfoMap)[indexFileName]
		if remoteIndexFileOk {
			hasNotBeenModified := reflect.DeepEqual(indexFileMetaData.GetBlockHashList(), remoteFileMetaData.GetBlockHashList())
			if !hasNotBeenModified {
				if remoteFileMetaData.GetVersion() == indexFileMetaData.GetVersion() {
					indexFileMetaData.Version += 1
					returnVersion = UpdateRemoteServer(client, indexFileMetaData)
				} else if remoteFileMetaData.GetVersion() > indexFileMetaData.GetVersion() {
					DownloadLatestVersion(client, remoteFileMetaData, indexFileInfoMap)
					continue
				}
			} else {
				continue
			}
		} else {
			indexFileMetaData.Version += 1
			returnVersion = UpdateRemoteServer(client, indexFileMetaData)
		}
		// upload failed. Download latest version from the server and
		// update index.db and the file in base directory
		if -1 == returnVersion {
			DownloadLatestVersion(client, remoteFileMetaData, indexFileInfoMap)
		}

	}

	// sync remote index and local index
	// a file existed in the server that wasn’t locally present
	for remoteFileName, remoteFileMetaData := range *remoteFileInfoMap {
		_, indexFileOk := (*indexFileInfoMap)[remoteFileName]
		if !indexFileOk {
			(*indexFileInfoMap)[remoteFileName] = remoteFileMetaData
			DownloadLatestVersion(client, remoteFileMetaData, indexFileInfoMap)
		}

	}

}

func IndexAndBaseDirFilesSync(baseDirFileInfoMap *map[string]*FileMetaData,
	indexFileInfoMap *map[string]*FileMetaData) {

	// insertion and update
	for baseDirFileName, baseDirFileMetaData := range *baseDirFileInfoMap {
		indexFileMetaData, indexFileOk := (*indexFileInfoMap)[baseDirFileName]
		if indexFileOk {
			// if the file in the base directory exists in the index.db,
			// check if it has been modified
			hasNotBeenModified := reflect.DeepEqual((*indexFileInfoMap)[baseDirFileName].GetBlockHashList(), (*baseDirFileInfoMap)[baseDirFileName].GetBlockHashList())
			if !hasNotBeenModified {
				baseDirFileMetaData.Version = indexFileMetaData.GetVersion()
				indexFileMetaData = baseDirFileMetaData
			} else {
				baseDirFileMetaData.Version = indexFileMetaData.GetVersion()
			}
			// fmt.Println("is 1 true? ", reflect.DeepEqual((*indexFileInfoMap)[baseDirFileName].GetBlockHashList(), indexFileMetaData.GetBlockHashList()))
			// fmt.Println("is 2 true? ", reflect.DeepEqual(indexFileMetaData, baseDirFileMetaData))
			(*indexFileInfoMap)[baseDirFileName] = indexFileMetaData
			(*baseDirFileInfoMap)[baseDirFileName] = baseDirFileMetaData
			// fmt.Println("is 3 true? ", reflect.DeepEqual((*indexFileInfoMap)[baseDirFileName].GetBlockHashList(), indexFileMetaData.GetBlockHashList()))
		} else {
			(*indexFileInfoMap)[baseDirFileName] = baseDirFileMetaData
		}
	}

	// deletion
	for indexFileName, indexFileMetaData := range *indexFileInfoMap {
		_, baseDirFileOk := (*baseDirFileInfoMap)[indexFileName]
		if !baseDirFileOk {
			// if the file in the index.db does NOT exist in the base directory,
			// check if it has already been deleted.
			// If not, update the version. Otherwise, don't change the version.
			// if len(indexFileMetaData.GetBlockHashList()) != 1 || indexFileMetaData.GetBlockHashList()[0] != "0" {
			// 	indexFileMetaData.Version += 1
			// }
			indexFileMetaData.BlockHashList = []string{"0"}
			(*indexFileInfoMap)[indexFileName] = indexFileMetaData

		}
	}

}

func CreateHashList(file *os.File, fileInfo fs.FileInfo, blockSize int) []string {
	hashList := make([]string, 0)
	buffer := make([]byte, blockSize)
	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return []string{"-1"}
	}
	for {
		if fileSize < int64(blockSize) {
			break
		}
		n, err := io.ReadFull(file, buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErr(err)
		}
		hashVal := GetBlockHashString(buffer[:n])
		hashList = append(hashList, hashVal)
		fileSize -= int64(n)
	}
	newBuffer := make([]byte, fileSize)
	n, err := io.ReadFull(file, newBuffer)
	if err != nil {
		if err == io.EOF {
			return hashList
		}
		checkErr(err)
	}
	hashVal := GetBlockHashString(newBuffer[:n])
	hashList = append(hashList, hashVal)

	return hashList
}

func CreateFileMetaData(fileInfo fs.FileInfo, fileMetaData *FileMetaData, blockSize int, file *os.File) {
	fileMetaData.Filename = fileInfo.Name()
	fileMetaData.Version = 0
	fileHashList := CreateHashList(file, fileInfo, blockSize)
	fileMetaData.BlockHashList = fileHashList
	// fmt.Println("CreateFileMetaData, fileHashListLen = ", len(fileHashList))
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// The client should first scan the base directory, and for each file,
	// compute that file’s hash list. The client should then consult the local index file
	// and compare the results, to see whether
	// (1) there are now new files in the base directory that aren’t in the index file, or
	// (2) files that are in the index file, but have changed since the last time
	//     the client was executed (i.e., the hash list is different).

	// index.db represents the state of your local base dir right as your client is exiting.
	// So first scan the files, then try to upload to the cloud, then write out your index.db

	// scan the base directory
	dirEntries, err := os.ReadDir(client.BaseDir)
	checkErr(err)
	// Create base directory file meta data map
	baseDirFileInfoMap := make(map[string]*FileMetaData)
	for _, dirEntry := range dirEntries {
		if !dirEntry.IsDir() {
			fileInfo, err := dirEntry.Info()
			checkErr(err)
			if fileInfo.Name() == "index.db" {
				continue
			}
			filePath := ConcatPath(client.BaseDir, fileInfo.Name())
			file, err := os.Open(filePath)
			checkErr(err)
			// var fileMetaData *FileMetaData
			fileMetaData := new(FileMetaData)
			CreateFileMetaData(fileInfo, fileMetaData, client.BlockSize, file)
			baseDirFileInfoMap[dirEntry.Name()] = fileMetaData
		}
	}

	// Create file metadata map
	indexFileInfoMap := make(map[string]*FileMetaData)

	// Check if the index.db file exists, if not, create index.db
	// indexFilePath, err := filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
	// checkErr(err)

	// // var db *sql.DB
	// // _, err = os.Stat(indexFilePath)
	// // if os.IsNotExist(err) {
	// // 	db, err = sql.Open("sqlite3", indexFilePath)
	// // 	checkErr(err)
	// // }

	// indexDB, err := sql.Open("sqlite3", indexFilePath)
	// checkErr(err)

	indexFileInfoMap, err = LoadMetaFromMetaFile(client.BaseDir)
	checkErr(err)
	// fmt.Println("before: indexFileInfoMap = ", indexFileInfoMap)

	IndexAndBaseDirFilesSync(&baseDirFileInfoMap, &indexFileInfoMap)
	// fmt.Println("middle: indexFileInfoMap = ", indexFileInfoMap)

	// create remote file info map
	remoteFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteFileInfoMap)
	checkErr(err)

	LocalRemoteSync(&baseDirFileInfoMap, &indexFileInfoMap, &remoteFileInfoMap, client)
	// fmt.Println("after: indexFileInfoMap = ", indexFileInfoMap)

	// write indexFileInfoMap back to index.db
	WriteMetaFile(indexFileInfoMap, client.BaseDir)

}
