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

package mongodb

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2"
)

var mongoDBConf *MongoDBConf

func TestMain(m *testing.M) {
	// Read the core.yaml file for default config.
	ledgertestutil.SetupCoreYAMLConfig()

	viper.Set("ledger.state.mongoDBConfig.url", "localhost:27017")
	viper.Set("ledger.state.mongoDBConfig.username", "admin")
	viper.Set("ledger.state.mongoDBConfig.password", "123456")
	mongoDBConf = GetMongoDBConf()
	result := m.Run()
	os.Exit(result)
}

func TestMongoDBConnect(t *testing.T){
	dialInfo := &mgo.DialInfo{
        Addrs:     []string{"localhost:27017"},
		Username:mongoDBConf.UserName,
		Password:mongoDBConf.Password,}
	session, err := mgo.DialWithInfo(dialInfo)
	testutil.AssertNoError(t, err, "")
	session.DB("test")
}

func TestMongoDBQueryDocumentPagingComplex(t *testing.T) {
	dialInfo := &mgo.DialInfo{
        Addrs:     []string{mongoDBConf.Url},
		Username:mongoDBConf.UserName,
		Password:mongoDBConf.Password,}
	session, err := mgo.DialWithInfo(dialInfo)
	testutil.AssertNoError(t, err, "")

	db := session.DB(mongoDBConf.DBName)
	mongoDB := &MongoDB{db, mongoDBConf}

	query := "{\"owner\":\"tom1\"}"
	queryBson, err := GetQueryBson("ns2", query)
	testutil.AssertNoError(t, err, "")

	pageInfo := &PagingOrQuery{
		PagingInfo: &PagingInfo{
			TotalCount:          0,
			TotalPage:           0,
			LastPageRecordCount: 0,
			CurrentPageNum:      1,
			PageSize:            30,
			LastQueryPageNum:    0,
			LastQueryObjectId:   "",
			LastRecordObjectId:  "",
			SortBy:              "_id",
		},
		Query: queryBson,
	}

	pageResult, _, err := mongoDB.QueryDocumentPagingComplex(pageInfo)
	testutil.AssertNoError(t, err, "")

	testutil.AssertEquals(t, pageResult.LastQueryPageNum, 0)
}

func Benchmark_MongoDBQueryDocumentPagingComplex(b *testing.B) {
	b.StopTimer()

	dialInfo := &mgo.DialInfo{
        Addrs:     []string{mongoDBConf.Url},
        Direct:    false,
     	PoolLimit: 4096, // Session.SetPoolLimit   
		Username:mongoDBConf.UserName,
		Password:mongoDBConf.Password,}
	session, err := mgo.DialWithInfo(dialInfo)
	testutil.AssertNoError(b, err, "")

	db := session.DB(mongoDBConf.DBName)
	mongoDB := &MongoDB{db, mongoDBConf}
	testutil.AssertNoError(b, err, "")

	query := "{\"owner\":\"fred\"}"
	queryBson, err := GetQueryBson("ns2", query)

	pageInfo := &PagingOrQuery{
		PagingInfo: &PagingInfo{
			TotalCount:          0,
			TotalPage:           0,
			LastPageRecordCount: 0,
			CurrentPageNum:      1,
			PageSize:            30,
			LastQueryPageNum:    0,
			LastQueryObjectId:   "",
			LastRecordObjectId:  "",
			SortBy:              "_id",
		},
		Query: queryBson,
	}

	pageResult, docs, err := mongoDB.QueryDocumentPagingComplex(pageInfo)
	testutil.AssertNoError(b, err, "")
	pageResultJson, _ := json.Marshal(docs)
	logger.Infof(string(pageResultJson))

	pageInfo.PagingInfo.TotalPage = pageResult.TotalPage
	pageInfo.PagingInfo.TotalCount = pageResult.TotalCount
	_, pageInfo.PagingInfo.LastPageRecordCount = GetPage(pageInfo.PagingInfo.TotalCount, pageInfo.PagingInfo.PageSize)
	pageInfo.PagingInfo.CurrentPageNum = 1
	pageInfo.PagingInfo.LastQueryPageNum = pageResult.LastQueryPageNum
	pageInfo.PagingInfo.LastQueryObjectId = pageResult.LastQueryObjectId
	pageInfo.PagingInfo.LastRecordObjectId = pageResult.LastRecordObjectId

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		pageResult, docs, err = mongoDB.QueryDocumentPagingComplex(pageInfo)
	}
	testutil.AssertNoError(b, err, "")

}

func TestMongoDBOpen(t *testing.T) {
	dialInfo := &mgo.DialInfo{
        Addrs:     []string{mongoDBConf.Url},
        Direct:    false,
     	PoolLimit: 4096, // Session.SetPoolLimit   
		Username:mongoDBConf.UserName,
		Password:mongoDBConf.Password,}
	session, err := mgo.DialWithInfo(dialInfo)
	testutil.AssertNoError(t, err, "")

	db := session.DB(mongoDBConf.DBName)
	mongoDB := &MongoDB{db, mongoDBConf}
	mongoDB.Open()
	defer mongoDB.Close()

	jsonValue1 := "{\"asset_name\": \"marble1\",\"color\": \"blue\",\"size\": 1,\"owner\": \"tom\"}"
	doc := MongodbDoc{}
	doc.Key = "key1"
	doc.ChaincodeId = "mycc"
	doc.Value = jsonValue1
	mongoDB.SaveDoc(doc)
}

func BenchmarkMongoDB_SaveDoc(b *testing.B) {
	b.StopTimer()

	dialInfo := &mgo.DialInfo{
        Addrs:     []string{mongoDBConf.Url},
        Direct:    false,
     	PoolLimit: 4096, // Session.SetPoolLimit   
		Username:mongoDBConf.UserName,
		Password:mongoDBConf.Password,}
	session, err := mgo.DialWithInfo(dialInfo)
	testutil.AssertNoError(b, err, "")

	db := session.DB(mongoDBConf.DBName)
	mongoDB := &MongoDB{db, mongoDBConf}

	mongoDB.Open()
	defer mongoDB.Close()

	jsonValue1 := "{\"asset_name\": \"marble1\",\"color\": \"blue\",\"size\": 1,\"owner\": \"tom1\"}"
	doc := MongodbDoc{}
	doc.Key = "key2"
	doc.ChaincodeId = "mycc"
	doc.Value = jsonValue1
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		mongoDB.SaveDoc(doc)
	}
}
