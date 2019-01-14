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
	"encoding/json"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/util/mongodb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"gopkg.in/mgo.v2"
)

var logger = flogging.MustGetLogger("statemongodb")

var savePointKey = "statedb_savepoint"
var savePointNs = "savepoint"

var queryskip = 0

type VersionedDBProvider struct {
	session   *mgo.Session
	databases map[string]*VersionedDB
	mux       sync.Mutex
}

type VersionedDB struct {
	mongoDB *mongodb.MongoDB
	dbName  string
}


func NewVersionedDBProvider() (*VersionedDBProvider, error) {

	logger.Debugf("constructing MongoDB VersionedDBProvider")

	mongodbConf := mongodb.GetMongoDBConf()
	dialInfo, err := mgo.ParseURL(mongodbConf.Url)
	if err != nil {
		return nil, err
	}
	dialInfo.Timeout = mongodbConf.RequestTimeout
	dialInfo.Username = mongodbConf.UserName
	dialInfo.Password = mongodbConf.Password
	mgoSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}

	return &VersionedDBProvider{
		session:   mgoSession,
		databases: make(map[string]*VersionedDB),
		mux:       sync.Mutex{},
	}, nil
}

//TODO retry while get error
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()

	db, ok := provider.databases[dbName]
	if ok {
		return db, nil
	}

	vdr, err := newVersionDB(provider.session, dbName)
	if err != nil {
		return nil, err
	}

	return vdr, nil
}

func (provider *VersionedDBProvider) Close() {
	provider.session.Close()
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	doc, err := vdb.mongoDB.GetDoc(namespace, key)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	versionedV := statedb.VersionedValue{Value: nil, Version: &doc.Version}
	if doc.Value == nil && doc.Attachments != nil {
		versionedV.Value = doc.Attachments.AttachmentBytes
	} else {
		tempbyte, _ := json.Marshal(doc.Value)
		versionedV.Value = tempbyte
	}

	return &versionedV, nil
}

// GetVersion implements method in VersionedDB interface
//TODO support GetVersion
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	versionedValue, err := vdb.GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	if versionedValue == nil {
		return nil, nil
	}
	return versionedValue.Version, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	querylimit := vdb.mongoDB.Conf.QueryLimit
	dbItr := vdb.mongoDB.GetIterator(namespace, startKey, endKey, querylimit, queryskip)
	return newKVScanner(dbItr, namespace), nil
}

//实现书签方式 
func (vdb *VersionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	querylimit := vdb.mongoDB.Conf.QueryLimit
	dbItr := vdb.mongoDB.GetIterator(namespace, startKey, endKey, querylimit, queryskip)
	return newKVScanner(dbItr, namespace), nil
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, queryOrPaingStr string) (statedb.ResultsIterator, error) {
	queryByte := []byte(queryOrPaingStr)
	if !isJson(queryByte) {
		return nil, fmt.Errorf("the queryOrPaingStr is not a json : %s", queryOrPaingStr)
	}

	paingOrQuery := &mongodb.PagingOrQuery{}
	
	err := json.Unmarshal(queryByte, paingOrQuery)
	if err != nil {
		return nil, fmt.Errorf("the queryOrPaingStr string is not a pagingOrQuery json string:" + err.Error())
	}
	
	pagingInfo := paingOrQuery.PagingInfo
	//execute normal json query when paginginfo is nil
	//execute paging query when paginginfo is not nil
	if pagingInfo == nil {
		return vdb.JsonQuery(namespace, queryOrPaingStr)
	} else {
		return vdb.PagingQuery(namespace, paingOrQuery)
	}
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
		return nil,nil
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	namespaces := batch.GetUpdatedNamespaces()
	var out interface{}
	var err error
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			logger.Debugf("Channel [%s]: Applying key=[%#v]", vdb.dbName, k)
			if vv.Value == nil {
				vdb.mongoDB.Delete(ns, k)
			} else {
				doc := mongodb.MongodbDoc{
					Key:         k,
					Version:     *vv.Version,
					ChaincodeId: ns,
				}

				if !isJson(vv.Value) {
					logger.Debugf("Not a json, write it to the attachment ")
					attachmentByte := mongodb.Attachment{
						AttachmentBytes: vv.Value,
					}
					doc.Attachments = &attachmentByte
					doc.Value = nil
				} else {
					err = json.Unmarshal(vv.Value, &out)
					if err != nil {
						logger.Errorf("Error rises while unmarshal the vv.value, error :%s", err.Error())
					}
					doc.Value = out
				}

				err = vdb.mongoDB.SaveDoc(doc)
				if err != nil {
					logger.Errorf("Error during Commit(): %s\n", err.Error())
				}
			}
		}
	}

	err = vdb.recordSavepoint(height)
	if err != nil {
		logger.Errorf("Error during recordSavepoint : %s\n", err.Error())
		return err
	}

	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {

	doc, err := vdb.mongoDB.GetDoc(savePointNs, savePointKey)
	if err != nil {
		logger.Errorf("Error during get latest save point key, error : %s", err.Error())
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	return &doc.Version, nil
}

// ValidateKey implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKey(key string) error {
	if !utf8.ValidString(key) {
		return fmt.Errorf("Key should be a valid utf8 string: [%x]", key)
	}
	return nil
}

// BytesKeySuppoted implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySuppoted() bool {
	return false
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {

}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	if !utf8.ValidString(key) {
		return fmt.Errorf("Key should be a valid utf8 string: [%x]", key)
	}

	// MongoDB supports any bytes for the value
	return nil
}

func (vdb *VersionedDB) recordSavepoint(height *version.Height) error {
	mddoc,err := vdb.mongoDB.GetDoc(savePointNs, savePointKey)
	if err != nil {
		logger.Debugf(err.Error())
	}
	if mddoc != nil {
		err := vdb.mongoDB.Delete(savePointNs, savePointKey)
		if err != nil {
			logger.Debugf(err.Error())
		}
	}

	doc := mongodb.MongodbDoc{}
	doc.Version.BlockNum = height.BlockNum
	doc.Version.TxNum = height.TxNum
	doc.Value = nil
	doc.ChaincodeId = savePointNs
	doc.Key = savePointKey

	err = vdb.mongoDB.SaveDoc(doc)
	if err != nil {
		logger.Errorf("Error during update savepoint , error : %s", err.Error())
		return err
	}

	return err
}

// return paging result of json query
func (vdb *VersionedDB) PagingQuery(namespace string, pagingOrQuery *mongodb.PagingOrQuery) (statedb.ResultsIterator, error) {
	queryInterface := pagingOrQuery.Query
	queryStr, _ := json.Marshal(queryInterface)
	queryBson, err := mongodb.GetQueryBson(namespace, string(queryStr))
	pagingOrQuery.Query = queryBson

	pagingRes, docs, err := vdb.mongoDB.QueryDocumentPagingComplex(pagingOrQuery)
	if err != nil {
		return nil, err
	}

	return newPagingScanner(docs, pagingRes), nil
}

func (vdb *VersionedDB) JsonQuery(namespace, query string) (statedb.ResultsIterator, error) {
	
	queryBson, err := mongodb.GetQueryBson(namespace, query)
	if err != nil {
		return nil, err
	}

	var result *mgo.Iter
	result, err = vdb.mongoDB.QueryDocuments(queryBson)
	if err != nil {
		return nil, err
	}
	return newKVScanner(result, namespace), nil
}

type kvScanner struct {
	namespace string
	result    *mgo.Iter
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error) {
	doc := mongodb.MongodbDoc{}
	if !scanner.result.Next(&doc) {
		return nil, nil
	}

	blockNum := doc.Version.BlockNum
	txNum := doc.Version.TxNum
	height := version.NewHeight(blockNum, txNum)
	key := doc.Key
	value := doc.Value
	valueContent := []byte{}
	if doc.Value != nil {
		valueContent, _ = json.Marshal(value)
	} else {
		valueContent = doc.Attachments.AttachmentBytes
	}

	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: statedb.VersionedValue{Value: valueContent, Version: height},
	}, nil
}

func (scanner *kvScanner) Close() {
	err := scanner.result.Close()
	if err != nil {
		logger.Errorf("Error during close the iterator of scanner error : %s", err.Error())
	}
}

func (scanner *kvScanner) GetBookmarkAndClose() string {
	return ""
}

func newKVScanner(iter *mgo.Iter, namespace string) *kvScanner {
	return &kvScanner{namespace: namespace, result: iter}
}

type pagingScanner struct {
	cursor       int
	docs         []*mongodb.MongodbResultDoc
	pagingResult *mongodb.PageResult
}

func (scanner *pagingScanner) Next() (statedb.QueryResult, error) {

	scanner.cursor++

	if scanner.cursor >= len(scanner.docs) {
		return nil, nil
	}

	doc := scanner.docs[scanner.cursor]
	returnVersion := doc.Version
	var returnValue []byte
	value := doc.Value

	// return paging query info in first result
	if scanner.cursor == 0 {
		tmpValue := mongodb.PagingDoc{
			ReturnValue:      value,
			ReturnPageResult: scanner.pagingResult,
		}
		returnValue, _ = json.Marshal(tmpValue)
	} else {
		tmpValue, _ := json.Marshal(value)
		returnValue = tmpValue
	}

	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: doc.ChaincodeId, Key: doc.Key},
		VersionedValue: statedb.VersionedValue{Value: returnValue, Version: &returnVersion},
	}, nil
}

func (scanner *pagingScanner) Close() {
	scanner = nil
}

func (scanner *pagingScanner) GetBookmarkAndClose() string {
	return ""
}


func newPagingScanner(docs []*mongodb.MongodbResultDoc, pagingResult *mongodb.PageResult) *pagingScanner {
	return &pagingScanner{
		cursor:       -1,
		docs:         docs,
		pagingResult: pagingResult,
	}
}

//get or create newVersionDB
func newVersionDB(mgoSession *mgo.Session, dbName string) (*VersionedDB, error) {
	db := mgoSession.DB(dbName)
	conf := mongodb.GetMongoDBConf()
	conf.DBName = dbName
	MongoDB := &mongodb.MongoDB{db, conf}

	collectionsName, err := db.CollectionNames()
	if err != nil {
		return nil, err
	}

	//create default collection when it no exists
	if !contains(collectionsName, conf.CollectionName) {
		err := MongoDB.CreateCollection()
		if err != nil {
			return nil, err
		}
	}

	//build default index of {KEY,NS} when it not exists
	err = MongoDB.BuildDefaultIndexIfNotExisted()
	if err != nil {
		return nil, err
	}

	return &VersionedDB{MongoDB, dbName}, nil
}

func isJson(value []byte) bool {
	var result interface{}
	err := json.Unmarshal(value, &result)
	return err == nil
}

func contains(src []string, value string) bool {

	if src == nil {
		return false
	}

	for _, v := range src {
		if v == value {
			return true
		}
	}

	return false
}


// BytesKeySupported implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySupported() bool {
	return false
}
