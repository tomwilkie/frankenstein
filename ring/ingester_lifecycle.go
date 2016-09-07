// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Responsible for managing the ingester lifecycle.

package ring

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/common/log"
)

const (
	infName           = "eth0"
	ring              = "ring"
	heartbeatInterval = 5 * time.Second
)

// IngesterRegistration manages the connection between the ingester and Consul.
type IngesterRegistration struct {
	consul    ConsulClient
	numTokens int

	id       string
	hostname string
	quit     chan struct{}
	wait     sync.WaitGroup
}

// RegisterIngester registers an ingester with Consul.
func RegisterIngester(consulClient ConsulClient, listenPort, numTokens int) (*IngesterRegistration, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	addr, err := getFirstAddressOf(infName)
	if err != nil {
		return nil, err
	}

	r := &IngesterRegistration{
		consul:    consulClient,
		numTokens: numTokens,

		id:       hostname,
		hostname: fmt.Sprintf("%s:%d", addr, listenPort),
		quit:     make(chan struct{}),
	}

	r.wait.Add(1)
	go r.loop()
	return r, nil
}

func (r *IngesterRegistration) loop() {
	defer r.wait.Done()
	tokens := r.pickTokens()
	defer r.unregister(tokens)
	r.heartbeat(tokens)
}

func (r *IngesterRegistration) pickTokens() []uint32 {
	var tokens []uint32
	if err := r.consul.CAS(ring, descFactory, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = newDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		takenTokens := []uint32{}
		for _, token := range ringDesc.Tokens {
			takenTokens = append(takenTokens, token.Token)
		}
		tokens = generateTokens(r.numTokens, takenTokens)

		populateRingDesc(ringDesc, r.id, r.hostname, tokens)

		return ringDesc, true, nil
	}); err != nil {
		log.Fatalf("Failed to pick tokens in consul: %v", err)
	}
	return tokens
}

func (r *IngesterRegistration) heartbeat(tokens []uint32) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Infof("Heartbeating to consul...")
			if err := r.consul.CAS(ring, descFactory, func(in interface{}) (out interface{}, retry bool, err error) {
				ringDesc := &Desc{}
				if in != nil {
					ringDesc = in.(*Desc)
				}

				ingesterDesc, ok := ringDesc.Ingesters[r.id]
				if !ok {
					// consul must have restarted
					log.Infof("Found empty ring, inserting tokens!")
					populateRingDesc(ringDesc, r.id, r.hostname, tokens)
				} else {
					ingesterDesc.Timestamp = time.Now()
					ringDesc.Ingesters[r.id] = ingesterDesc
				}

				return ringDesc, true, nil
			}); err != nil {
				log.Errorf("Failed to write to consul, sleeping: %v", err)
			}
		case <-r.quit:
			return
		}
	}
}

// Unregister deletes ingestor config from Consul
func (r *IngesterRegistration) Unregister() {
	log.Info("Removing ingester from consul")
	close(r.quit)
	r.wait.Wait()
}

func (r *IngesterRegistration) unregister(tokens []uint32) {
	if err := r.consul.CAS(ring, descFactory, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			log.Error("Found empty ring when trying to unregister!")
			return nil, false, nil
		}

		ringDesc := in.(*Desc)
		removeFromRingDesc(ringDesc, r.id, tokens)
		return ringDesc, true, nil
	}); err != nil {
		log.Fatalf("Failed to unregister from consul: %v", err)
	}
}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

func generateTokens(numTokens int, takenTokens []uint32) []uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := sortableUint32{}
	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		j := sort.Search(len(takenTokens), func(i int) bool {
			return takenTokens[i] >= candidate
		})
		if j < len(takenTokens) && takenTokens[j] == candidate {
			continue
		}
		tokens = append(tokens, candidate)
		i++
	}
	sort.Sort(tokens)
	return tokens
}

func populateRingDesc(ringDesc *Desc, id, hostname string, tokens []uint32) {
	ringDesc.Ingesters[id] = IngesterDesc{
		Hostname:  hostname,
		Timestamp: time.Now(),
	}

	for _, token := range tokens {
		ringDesc.Tokens = append(ringDesc.Tokens, TokenDesc{
			Token:    token,
			Ingester: id,
		})
	}

	sort.Sort(ringDesc.Tokens)
}

func removeFromRingDesc(ringDesc *Desc, id string, tokens []uint32) {
	delete(ringDesc.Ingesters, id)
	output := []TokenDesc{}
	i, j := 0, 0
	for i < len(ringDesc.Tokens) && j < len(tokens) {
		if ringDesc.Tokens[i].Token < tokens[j] {
			output = append(output, ringDesc.Tokens[i])
			i++
		} else if ringDesc.Tokens[i].Token > tokens[j] {
			log.Infof("Missing token from ring: %d", tokens[j])
			j++
		} else {
			i++
			j++
		}
	}
	for i < len(ringDesc.Tokens) {
		output = append(output, ringDesc.Tokens[i])
		i++
	}
	for j < len(tokens) {
		log.Infof("Missing token from ring: %d", tokens[j])
		j++
	}
	ringDesc.Tokens = output
}

// getFirstAddressOf returns the first IPv4 address of the supplied interface name.
func getFirstAddressOf(name string) (string, error) {
	inf, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}

	addrs, err := inf.Addrs()
	if err != nil {
		return "", err
	}
	if len(addrs) <= 0 {
		return "", fmt.Errorf("No address found for %s", name)
	}

	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if ip := v.IP.To4(); ip != nil {
				return v.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("No address found for %s", name)
}
