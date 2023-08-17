package utils

import (
	"fmt"
	"os"
)

func ReadTextInt64FromFile(filename string) (int64, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return 0, err
	}
	var value int64
	_, err = fmt.Fscanf(file, "%d\n", &value)
	return value, nil
}

func WriteTextInt64ToFile(filename string, value int64) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	if err = file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "%d\n", value)
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
}

func ReadTextUint32FromFile(filename string) (uint32, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return 0, err
	}
	var value uint32
	_, err = fmt.Fscanf(file, "%d\n", &value)
	return value, nil
}

func WriteTextUint32ToFile(filename string, value uint32) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	if err = file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "%d\n", value)
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
}

func ExistFile(filename string) bool {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return fileInfo.IsDir() == false
}

func ExistDir(dirname string) bool {
	fileInfo, err := os.Stat(dirname)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()
}
