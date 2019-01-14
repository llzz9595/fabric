/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statemongodb

import (
	"os"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/commontests"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/hyperledger/fabric/common/ledger/testutil"
)

func TestMain(m *testing.M) {
	// Read the core.yaml file for default config.
	ledgertestutil.SetupCoreYAMLConfig()

	viper.Set("ledger.state.mongoDBConfig.url", "localhost:27017")
	viper.Set("ledger.state.mongoDBConfig.username", "admin")
	viper.Set("ledger.state.mongoDBConfig.password", "123456")
	result := m.Run()
	os.Exit(result)
}

func TestBasicRW(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testbasicrw")
	commontests.TestBasicRW(t, env.DBProvider)
}

func TestGetStateMultipleKeys(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testgetmultiplekeys")
	commontests.TestGetStateMultipleKeys(t, env.DBProvider)
}

func TestMultiDBBasicRW(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testmultidbbasicrw")
	defer env.Cleanup("testmultidbbasicrw2")
	commontests.TestMultiDBBasicRW(t, env.DBProvider)
}

func TestDeletes(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testdeletes")
	commontests.TestDeletes(t, env.DBProvider)
}

func TestIterator(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testiterator")
	commontests.TestIterator(t, env.DBProvider)
}

func TestJsonQuery(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testquery")
	commontests.TestMongoQuery(t, env.DBProvider)
}

func TestPaging(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("")
	commontests.TestPaingQuery(t, env.DBProvider)
}

func TestQueryOrPaging(t *testing.T) {
	env := NewTestDBEnv(t)
	defer env.Cleanup("testpaging")
	commontests.TestExecuteQueryPaging(t, env.DBProvider)
}



func TestHandleChaincodeDeploy(t *testing.T) {
	
		env := NewTestDBEnv(t)
		env.Cleanup("testinit_")
		env.Cleanup("testinit_ns1")
		env.Cleanup("testinit_ns2")
		defer env.Cleanup("testinit_")
		defer env.Cleanup("testinit_ns1")
		defer env.Cleanup("testinit_ns2")
	
		db, err := env.DBProvider.GetDBHandle("testinit")
		assert.NoError(t, err, "")
		db.Open()
	
		defer db.Close()
		batch := statedb.NewUpdateBatch()
	
		jsonValue1 := "{\"asset_name\": \"marble1\",\"color\": \"blue\",\"size\": 1,\"owner\": \"tom\"}"
		batch.Put("ns1", "key1", []byte(jsonValue1), version.NewHeight(1, 1))
		jsonValue2 := "{\"asset_name\": \"marble2\",\"color\": \"blue\",\"size\": 2,\"owner\": \"jerry\"}"
		batch.Put("ns1", "key2", []byte(jsonValue2), version.NewHeight(1, 2))
		jsonValue3 := "{\"asset_name\": \"marble3\",\"color\": \"blue\",\"size\": 3,\"owner\": \"fred\"}"
		batch.Put("ns1", "key3", []byte(jsonValue3), version.NewHeight(1, 3))
		jsonValue4 := "{\"asset_name\": \"marble4\",\"color\": \"blue\",\"size\": 4,\"owner\": \"martha\"}"
		batch.Put("ns1", "key4", []byte(jsonValue4), version.NewHeight(1, 4))
		jsonValue5 := "{\"asset_name\": \"marble5\",\"color\": \"blue\",\"size\": 5,\"owner\": \"fred\"}"
		batch.Put("ns1", "key5", []byte(jsonValue5), version.NewHeight(1, 5))
		jsonValue6 := "{\"asset_name\": \"marble6\",\"color\": \"blue\",\"size\": 6,\"owner\": \"elaine\"}"
		batch.Put("ns1", "key6", []byte(jsonValue6), version.NewHeight(1, 6))
		jsonValue7 := "{\"asset_name\": \"marble7\",\"color\": \"blue\",\"size\": 7,\"owner\": \"fred\"}"
		batch.Put("ns1", "key7", []byte(jsonValue7), version.NewHeight(1, 7))
		jsonValue8 := "{\"asset_name\": \"marble8\",\"color\": \"blue\",\"size\": 8,\"owner\": \"elaine\"}"
		batch.Put("ns1", "key8", []byte(jsonValue8), version.NewHeight(1, 8))
		jsonValue9 := "{\"asset_name\": \"marble9\",\"color\": \"green\",\"size\": 9,\"owner\": \"fred\"}"
		batch.Put("ns1", "key9", []byte(jsonValue9), version.NewHeight(1, 9))
		jsonValue10 := "{\"asset_name\": \"marble10\",\"color\": \"green\",\"size\": 10,\"owner\": \"mary\"}"
		batch.Put("ns1", "key10", []byte(jsonValue10), version.NewHeight(1, 10))
		jsonValue11 := "{\"asset_name\": \"marble11\",\"color\": \"cyan\",\"size\": 1000007,\"owner\": \"joe\"}"
		batch.Put("ns1", "key11", []byte(jsonValue11), version.NewHeight(1, 11))
	
		//add keys for a separate namespace
		batch.Put("ns2", "key1", []byte(jsonValue1), version.NewHeight(1, 12))
		batch.Put("ns2", "key2", []byte(jsonValue2), version.NewHeight(1, 13))
		batch.Put("ns2", "key3", []byte(jsonValue3), version.NewHeight(1, 14))
		batch.Put("ns2", "key4", []byte(jsonValue4), version.NewHeight(1, 15))
		batch.Put("ns2", "key5", []byte(jsonValue5), version.NewHeight(1, 16))
		batch.Put("ns2", "key6", []byte(jsonValue6), version.NewHeight(1, 17))
		batch.Put("ns2", "key7", []byte(jsonValue7), version.NewHeight(1, 18))
		batch.Put("ns2", "key8", []byte(jsonValue8), version.NewHeight(1, 19))
		batch.Put("ns2", "key9", []byte(jsonValue9), version.NewHeight(1, 20))
		batch.Put("ns2", "key10", []byte(jsonValue10), version.NewHeight(1, 21))
	
		savePoint := version.NewHeight(2, 22)
		db.ApplyUpdates(batch, savePoint)
	
		//Create a tar file for test with 4 index definitions and 2 side dbs
		dbArtifactsTarBytes := testutil.CreateTarBytesForTest(
			[]*testutil.TarFileEntry{
				{"META-INF/statedb/couchdb/indexes/indexColorSortName.json", `{"index":{"fields":[{"color":"desc"}]},"ddoc":"indexColorSortName","name":"indexColorSortName","type":"json"}`},
				{"META-INF/statedb/couchdb/indexes/indexSizeSortName.json", `{"index":{"fields":[{"size":"desc"}]},"ddoc":"indexSizeSortName","name":"indexSizeSortName","type":"json"}`},
				{"META-INF/statedb/couchdb/collections/collectionMarbles/indexes/indexCollMarbles.json", `{"index":{"fields":["docType","owner"]},"ddoc":"indexCollectionMarbles", "name":"indexCollectionMarbles","type":"json"}`},
				{"META-INF/statedb/couchdb/collections/collectionMarblesPrivateDetails/indexes/indexCollPrivDetails.json", `{"index":{"fields":["docType","price"]},"ddoc":"indexPrivateDetails", "name":"indexPrivateDetails","type":"json"}`},
			},
		)
	
		//Create a query
		queryString :=  "{\"owner\":\"fred\"}"
		
		_, err = db.ExecuteQuery("ns1", queryString)
		assert.NoError(t, err, "")
	
	/* 	 //Create a query with a sort
		queryString = `{"owner":"fred"},{"size": "desc"}]}`
	
		_, err = db.ExecuteQuery("ns1", queryString)
		assert.Error(t, err, "Error should have been thrown for a missing index")  */
	
		indexCapable, ok := db.(statedb.IndexCapable)
	
		if !ok {
			t.Fatalf("Couchdb state impl is expected to implement interface `statedb.IndexCapable`")
		}
	
		fileEntries, errExtract := ccprovider.ExtractFileEntries(dbArtifactsTarBytes, "couchdb")
		assert.NoError(t, errExtract, "")
	
		indexCapable.ProcessIndexesForChaincodeDeploy("ns1", fileEntries["META-INF/statedb/couchdb/indexes"])
		//Sleep to allow time for index creation
		time.Sleep(100 * time.Millisecond)
		//Create a query with a sort
		queryString = `{"selector":{"owner":"fred"}, "sort": [{"size": "desc"}]}`
	
		//Query should complete without error
		_, err = db.ExecuteQuery("ns1", queryString)
		assert.NoError(t, err, "")
	
		//Query namespace "ns2", index is only created in "ns1".  This should return an error.
		_, err = db.ExecuteQuery("ns2", queryString)
		assert.Error(t, err, "Error should have been thrown for a missing index")
	
	}