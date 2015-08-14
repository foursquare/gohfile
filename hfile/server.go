// Copyright (C) 2014 Daniel Harrison

package hfile

import "encoding/json"
import "fmt"
import "io/ioutil"
import "net/http"
import "os"

type ServerConfigs struct {
	path   string
	Port   uint32
	HFiles []ServerConfig
}

type ServerConfig struct {
	Name string
	Path string
}

func (configs *ServerConfigs) String() string {
	raw, _ := json.Marshal(*configs)
	return string(raw)
}
func (configs *ServerConfigs) Set(path string) error {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &configs)
}

type ServerHandler struct {
	config ServerConfig
	hfile  *Reader
}

func (s ServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 401)
	} else {
		scan := NewScanner(s.hfile)
		value, err, found := scan.GetFirst(key)
		if found {
			fmt.Fprint(w, value)
		} else {
			if err != nil {
				http.Error(w, "not found", 500)
			} else {
				http.Error(w, "not found", 404)
			}
		}
	}
}

type Server struct {
	configs  ServerConfigs
	handlers []ServerHandler
}

func NewServer(configs ServerConfigs) (Server, error) {
	s := Server{configs: configs}
	fmt.Println(s.configs.String())
	for _, config := range configs.HFiles {
		handler := ServerHandler{config: config}
		file, err := os.OpenFile(handler.config.Path, os.O_RDONLY, 0)
		if err != nil {
			return s, err
		}
		handler.hfile, err = NewReader(file, false)
		if err != nil {
			return s, err
		}
		s.handlers = append(s.handlers, handler)
	}
	return s, nil
}
func (s *Server) Start() {
	for _, handler := range s.handlers {
		http.Handle(fmt.Sprintf("/get/%s", handler.config.Name), handler)
	}
	port := s.configs.Port
	if port == 0 {
		port = 4000
	}
	fmt.Println("Listening on port", port)
	http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil)
}
