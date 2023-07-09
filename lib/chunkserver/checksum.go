package chunkserver

import (
	"hash/crc32"
	"os"
)

type Checksum uint32

func (checksum *Checksum) UpdateWithByteString(data []byte) {
	*checksum = Checksum(crc32.ChecksumIEEE(data))
}

func (checksum *Checksum) Update(file *os.File) error {
	fileStatus, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStatus.Size()
	data := make([]byte, fileSize)
	_, err = file.Read(data)
	if err != nil {
		return err
	}
	checksum.UpdateWithByteString(data)
	return nil
}

func (checksum *Checksum) Check(data []byte) bool {
	return *checksum == Checksum(crc32.ChecksumIEEE(data))
}
