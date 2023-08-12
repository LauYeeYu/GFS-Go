package utils

import (
	"fmt"
	"os"
)

func ReadTextInt64FromFile(filename string) (int, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return 0, err
	}
	var value int
	_, err = fmt.Fscanf(file, "%d\n", &value)
	return value, nil
}

func WriteTextInt64ToFile(filename string, value int64) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	if file.Truncate(0) != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "%d\n", value)
	_ = file.Close()
	return nil
}