package bkorm

import (
    "fmt"
    "time"
    "strings"

    . "github.com/jabbawockeez/go-utils"
    json "github.com/jabbawockeez/go-simplejson"
    "github.com/go-bongo/bongo"
    "github.com/globalsign/mgo/bson"
    "github.com/globalsign/mgo"
)

type InstAsst struct {
    bongo.DocumentBase `bson:",inline"`
    
    ObjAsstId   string    `bson:"bk_obj_asst_id" json:"bk_obj_asst_id"`
    InstId      int       `bson:"bk_inst_id" json:"bk_inst_id"`
    AsstInstId  int       `bson:"bk_asst_inst_id" json:"bk_asst_inst_id"`
}

type ObjAsst struct {
    bongo.DocumentBase `bson:",inline"`
    
    Mapping  string    `bson:"mapping"`
    Ma       string    `bson:"bk_asst_id_test"`
    Ma2       string    `bson:"bk_obj_asst_id"`
}

const (
    TBNAME = "cc_InstAsst"
)

//var config = &bongo.Config{
//    ConnectionString: "bk-cmdb:bk-cmdb@10.10.133.11/cmdb",
//    //Database: "cmdb",
//    //DialInfo: &mgo.DialInfo{Database:"cmdb", Username:"bk-cmdb", Password:"bk-cmdb"},
//}
//var conn *bongo.Connection 

//func init() {
//    var err error
//    conn, err = bongo.Connect(config)
//    if err != nil {
//        fmt.Println(err)
//    }
//}
func newConn() (c *bongo.Connection) {
    var err error
    var split func(rune) bool = func(r rune) bool {
        return r == ':' || r == '@' || r == '/'
    }
    a := strings.FieldsFunc(apiconf.MongoConn, split)
    db := a[3]
    name := a[0]
    pwd := a[1]
    addr := a[2]
    c, err = bongo.Connect(&bongo.Config{
                //ConnectionString: apiconf.MongoConn,
                DialInfo: &mgo.DialInfo{Addrs:[]string{addr},
                                        Database:db, 
                                        Username:name,
                                        Password:pwd,
                                        Timeout: 3 * time.Second},
             })
    if err != nil {
        fmt.Println(err)
    }
    return
}

func Mgtest() {
    Pf("%s", "")
    //create()
    read()
}

func deleteFromMongo(data *json.Json) (err error) {
    asst := &InstAsst{}
    data.ToStruct(asst)

    conn := newConn()
    err = conn.Collection(TBNAME).DeleteDocument(asst)

    return
}

func getCurrentInstAsst(instId int) (result *json.Json) {
    result = json.FromString("[]")
    asst := &InstAsst{}

    conn := newConn()
    results := conn.Collection(TBNAME).Find(bson.M{"bk_inst_id":instId})

    for results.Next(asst) {
        result.Append("", asst)
    }

    return
}

//func getAsstMapping(asstId string) {
//    session, err := mgo.Dial(config.ConnectionString)
//    if err != nil {
//        panic(err)
//    }
//
//    defer session.Close()
//    session.SetMode(mgo.Monotonic, true)
//
//    c := session.DB("cmdb").C("cc_InstAsst")
//
//    objAssts := []*ObjAsst{}
//
//    c.Find(bson.M{"bk_obj_asst_id": asstId}).All(&objAssts)
//
//    P("Results : ", objAssts[0].Mapping, objAssts[0].Ma, objAssts[0].Ma2)
//
//}

func getAsstMapping(asstId string) {
    objAsst := new(ObjAsst)

    conn := newConn()
    results := conn.Collection("cc_InstAsst").Find(bson.M{"bk_obj_asst_id": asstId})
    P(results.Collection)

    for results.Next(objAsst) {
        //result.Append("", objAsst)
        P(objAsst.Mapping)
    }
}

func getCurrentInstRef(instId int, objId string) (result *json.Json) {
    result = json.FromString("[]")
    asst := &InstAsst{}

    // $or:
    // bson.M{ "$or": []bson.M{ bson.M{"uuid":"UUID0"}, bson.M{"name": "Joe"} } }

    condition := bson.M{
                        "bk_asst_inst_id": instId,
                        "bk_obj_asst_id": bson.RegEx{objId, ""},
                    }

    conn := newConn()
    results := conn.Collection(TBNAME).Find(condition)

    for results.Next(asst) {
        result.Append("", asst)
        //P(asst)
    }

    return
}

func read() {
    asst := &InstAsst{}

    conn := newConn()
    results := conn.Collection(TBNAME).Find(bson.M{"bk_inst_id":55339})
    //fmt.Println("---", results)

    for results.Next(asst) {
        fmt.Println(asst.InstId, asst.AsstInstId, asst.ObjAsstId)
    }
}

func create() {
    asst := &InstAsst{
                ObjAsstId: "bk_domain_default_bk_ngconf_template",
                InstId: 55339,
                AsstInstId: 73514,
            }

    Pf("%v", asst)
    conn := newConn()
    err := conn.Collection(TBNAME).Save(asst)
    if vErr, ok := err.(*bongo.ValidationError); ok {
        fmt.Println("Validation errors are:", vErr.Errors)
    } else {
        fmt.Println("Got a real error:", err.Error())
    }
}
