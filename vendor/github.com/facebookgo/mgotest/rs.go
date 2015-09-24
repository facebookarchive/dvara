package mgotest

import (
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	statePrimary   = 1
	stateSecondary = 2
)

type configMember struct {
	ID   int    `bson:"_id"`
	Host string `bson:"host"`
}

type config struct {
	ID      string         `bson:"_id"`
	Version int            `bson:"version,omitempty"`
	Members []configMember `bson:"members"`
}

type statusMember struct {
	Name  string `bson:"name"`
	State uint   `bson:"state"`
}

type status struct {
	ID      string         `bson:"_id"`
	Set     string         `bson:"set"`
	MyState uint           `bson:"myState"`
	Members []statusMember `bson:"members"`
}

// ReplicaSet provides a configured ReplicaSet.
type ReplicaSet struct {
	T       Fatalf
	Servers []*Server
}

// Stop the ReplicaSet.
func (r *ReplicaSet) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(r.Servers))
	for _, s := range r.Servers {
		go func(s *Server) {
			defer wg.Done()
			s.Stop()
		}(s)
	}
	wg.Wait()
}

// Addrs for all the servers.
func (r *ReplicaSet) Addrs() []string {
	var addrs []string
	for _, s := range r.Servers {
		addrs = append(addrs, s.URL())
	}
	return addrs
}

// Session for the mongo ReplicaSet.
func (r *ReplicaSet) Session() *mgo.Session {
	info := mgo.DialInfo{
		Addrs:   r.Addrs(),
		Timeout: 10 * time.Second,
	}
	session, err := mgo.DialWithInfo(&info)
	if err != nil {
		r.T.Fatalf(err.Error())
	}
	return session
}

// NewReplicaSet makes a new ReplicaSet with the given number of nodes.
func NewReplicaSet(num uint, tb Fatalf) *ReplicaSet {
	if num == 0 {
		tb.Fatalf("NewReplSet called with num=0")
	}

	rs := ReplicaSet{T: tb}
	for i := uint(0); i < num; i++ {
		rs.Servers = append(rs.Servers, NewReplSetServer(tb))
	}

	var members []configMember
	for i, s := range rs.Servers {
		members = append(members, configMember{
			ID:   i,
			Host: s.URL(),
		})
	}

	primary, err := mgo.Dial(rs.Servers[0].URL() + "?connect=direct")
	if err != nil {
		tb.Fatalf(err.Error())
	}
	primary.SetMode(mgo.Monotonic, true)

	var initiateResponse bson.M
	err = primary.Run(
		bson.M{
			"replSetInitiate": config{
				ID: "rs",
				Members: []configMember{
					{ID: int(0), Host: rs.Servers[0].URL()},
				},
			},
		},
		&initiateResponse,
	)
	if err != nil {
		tb.Fatalf(err.Error())
	}

	var statusResponse status
	for {
		err = primary.Run(
			bson.M{
				"replSetGetStatus": int(1),
			},
			&statusResponse,
		)
		if err != nil {
			if err.Error() == "Received replSetInitiate - should come online shortly." {
				time.Sleep(time.Second)
				continue
			}
			tb.Fatalf(err.Error())
		}
		if statusResponse.MyState != statePrimary {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	var reconfigResponse bson.M
	err = primary.Run(
		bson.M{
			"replSetReconfig": config{
				ID:      "rs",
				Version: 2,
				Members: members,
			},
		},
		&reconfigResponse,
	)
	if err != nil {
		tb.Fatalf(err.Error())
	}

outerStatusReadyCheckLoop:
	for {
		err = primary.Run(
			bson.M{
				"replSetGetStatus": int(1),
			},
			&statusResponse,
		)
		if err != nil {
			tb.Fatalf(err.Error())
		}
		for _, m := range statusResponse.Members {
			if m.State != statePrimary && m.State != stateSecondary {
				time.Sleep(time.Second)
				continue outerStatusReadyCheckLoop
			}
		}
		break
	}

	return &rs
}
