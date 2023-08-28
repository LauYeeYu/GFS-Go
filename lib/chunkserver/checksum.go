package chunkserver

import (
	"fmt"
	"gfs"
	"gfs/utils"
	"hash/crc32"
	"os"
)

type Checksum uint32

func (checksum *Checksum) UpdateWithByteString(data []byte) {
	*checksum = GetChecksum(data)
}

func GetChecksum(data []byte) Checksum {
	return Checksum(crc32.ChecksumIEEE(data))
}

func (checksum *Checksum) Update(file *os.File, checksumFilePath string) error {
	fileStatus, err := file.Stat()
	if err != nil {
		return gfs.NewFatalError(err)
	}
	fileSize := fileStatus.Size()
	data := make([]byte, fileSize)
	_, err = file.Read(data)
	if err != nil {
		return gfs.NewFatalError(err)
	}
	checksum.UpdateWithByteString(data)
	err = utils.WriteTextUint32ToFile(checksumFilePath, uint32(*checksum))
	if err != nil {
		return gfs.NewFatalError(err)
	}
	return nil
}

func checksumPath(storagePath string, handle gfs.ChunkHandle) string {
	return utils.MergePath(storagePath, fmt.Sprintf("%d%s", handle, gfs.ChecksumSuffix))
}

func (checksum *Checksum) Check(data []byte) bool {
	return *checksum == GetChecksum(data)
}
