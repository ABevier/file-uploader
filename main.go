package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/judwhite/go-svc/svc"
	"github.com/juju/fslock"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

type program struct {
	shutdown chan bool // Signals that shutdown is starting
	done     chan bool // Signals when shutdown is completed

	sourceDir    string
	completedDir string
	failedDir    string

	uploadURL string
}

func (p *program) processFiles(scanChannel <-chan string) {
	p.done = make(chan bool)

	go func() {
		defer close(p.done)

		for {
			filename, ok := <-scanChannel
			if !ok {
				return
			}
			if err := p.lockAndProcessFile(filename); err != nil {
				log.Println(err)
			}
		}
	}()
}

func (p *program) lockAndProcessFile(path string) error {
	log.Printf("Attempting to lock file %s", path)

	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if fi.Size() <= 0 {
		return nil
	}

	lock := fslock.New(path)
	i := 0
	for {
		if err := lock.TryLock(); err != nil {
			if i > 5 {
				return err
			}
			i++
		} else {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	lock.Unlock()

	return p.processFile(path)
}

func (p *program) processFile(path string) error {
	log.Printf("Processing file: %v\n", path)
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	mpw := multipart.NewWriter(w)
	go func() {
		defer file.Close()
		defer w.Close()

		part, err := mpw.CreateFormFile("file", filepath.Base(path))
		if err != nil {
			log.Printf("Failed to create body: %v", err)
			return
		}

		size, err := io.Copy(part, file)
		if err != nil {
			log.Printf("Failed to copy file: %v", err)
			return
		}
		log.Printf("Uploaded %v bytes for file %v", size, path)

		if err = mpw.Close(); err != nil {
			log.Printf("Failed to close Request: %v", err)
		}
	}()

	// Post to Server, goroutine above will pipe file contents to the request
	resp, err := http.Post(p.uploadURL, mpw.FormDataContentType(), r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		failedPath := filepath.Join(p.failedDir, filepath.Base(path))
		if err := os.Rename(path, failedPath); err != nil {
			log.Printf("Failed to move failed file: %v. %v", path, err)
			// continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Failed Upload. Status Code: %v. Could not parse body: %v", resp.StatusCode, err)
		}
		return fmt.Errorf("Failed Upload. Status Code: %v, Body: %v", resp.StatusCode, string(body))
	}

	// Success!
	newPath := filepath.Join(p.completedDir, filepath.Base(path))
	if err := os.Rename(path, newPath); err != nil {
		return fmt.Errorf("Failed to move completed file: %v", err)
	}
	return nil
}

func (p *program) timedScan() <-chan string {
	fileChannel := make(chan string)

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer close(fileChannel)

		for {
			select {
			case <-ticker.C:
				if err := p.scanDirectory(fileChannel); err != nil {
					log.Printf("Failed to read dir: %v", err)
				}
			case <-p.shutdown:
				return
			}
		}
	}()

	return fileChannel
}

func (p *program) scanDirectory(channel chan<- string) error {
	dir, err := os.Open(p.sourceDir)
	if err != nil {
		return err
	}

	files, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	for _, fi := range files {
		if !fi.IsDir() {
			filename := filepath.Join(p.sourceDir, fi.Name())
			channel <- filename
		}
	}
	return nil
}

func createURL() string {
	urlBase, err := url.Parse(viper.GetString("uploadURL"))
	if err != nil {
		log.Panicf("Cannot create url! %v", err)
	}

	return urlBase.String()
}

func (p *program) Init(env svc.Environment) error {
	viper.SetConfigName("conf")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		log.Panicf("Could not read conf.yml: %v", err)
	}

	p.sourceDir = viper.GetString("sourceDir")
	p.completedDir = viper.GetString("completedDir")
	p.failedDir = viper.GetString("failedDir")
	p.uploadURL = createURL()

	return nil
}

func (p *program) Start() error {
	log.Printf("Starting file-uploader.  SourceDir=%v, CompletedDir=%v, UploadUrl=%v",
		p.sourceDir, p.completedDir, p.uploadURL)

	go p.run()

	return nil
}

func (p *program) run() {
	if err := os.MkdirAll(p.sourceDir, os.ModePerm); err != nil {
		log.Panicf("Couldn't create source dir")
	}

	if err := os.MkdirAll(p.completedDir, os.ModePerm); err != nil {
		log.Panicf("Couldn't create completed dir")
	}

	if err := os.MkdirAll(p.failedDir, os.ModePerm); err != nil {
		log.Panicf("Couldn't create failed dir")
	}

	p.shutdown = make(chan bool)
	scanChannel := p.timedScan()

	p.processFiles(scanChannel)

	<-p.shutdown
	<-p.done
}

func (p *program) Stop() error {
	log.Println("Received shutdown signal.")

	close(p.shutdown)
	<-p.done

	log.Println("Shutdown complete")
	return nil
}

func main() {
	log.SetOutput(&lumberjack.Logger{
		Filename:   "./file-uploader.log",
		MaxSize:    10,
		MaxBackups: 3,
	})

	prg := &program{}
	if err := svc.Run(prg); err != nil {
		log.Panicf("Unable to run the service: %v", err)
	}
}
