package util

import (
	"os"
	"crypto/tls"
	"io/ioutil"
	"crypto/x509"
)

func FileExists(fileName string) (exists bool, err error) {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func GenTLSConfig(serverName, caFile, certFile, keyFile string) (config *tls.Config, err error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs: caCertPool,
		ServerName: serverName,
	}

	return config, nil
}
