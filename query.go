package bkorm

import (
    "fmt"
    //"strings"
    "sync"
    "reflect"
    //"unicode"

    "github.com/jabbawockeez/gorequest"
    json "github.com/jabbawockeez/go-simplejson"
    . "github.com/jabbawockeez/go-utils"
)

/*
extra options used by a queryset object(Orm.conf field):
    // http method
    Method

    // mark which current operation it is, 
    // should be one of "CREATE", "READ", "UPDATE", "DELETE"
    Operation

    // equals to bk_obj_id in bk cmdb
    ObjId

    // cache the current instance id
    *_InstId

    // if the EQ operator is used to match "bk_inst_name",
    // then the operator would be changed to REGEX, 
    // (it seems like a bug in bk cmdb, that self-defined object 
    // can not be searched by "bk_inst_name" while using "$eq"),
    // and this option would be set to "true"
    //R_ExactMatch     
    // contains the original inst name which used to find the exact object later
    //R_ExactMatchName

    // which fields are selected, the result will only contains these fields
    //R_Fields

    // if we want to read an instance with its 
    // association instances this will be true
    R_ReadAsst

    // the "*" represents the operation(C,R,U,D), 
    // this option is used to contain the object pointer 
    // which is used to hold something after the result is returned by bk
    *_obj_pointer
    
    // this is used to hold some temperory data 
    Temp

    // the "Asst" key in the "temp" section is used to hold the association 
    // information that would be updated after an instance is created or updated
    Temp:Asst
*/

const (
	// http methods
	GET = "GET"
	POST = "POST"
	PUT = "PUT"
	DELETE = "DELETE"

	// condition operators
	EQ = "$eq"
	NE = "$ne"
	REGEX = "$regex"

    // object operation
    C = "CREATE"
    R = "READ"
    U = "UPDATE"
    D = "DELETE"
)

type ApiConf struct {
    URL           string
    BK_USER       string
    SUPPLIER_ID   int

    MongoConn     string
}


type Orm struct {
	*json.Json          // hold data that would be send in POST and PUT
    conf    *json.Json  // hold some options for current request
}

var wg sync.WaitGroup
var Meta  *json.Json  // global meta data
var apiconf *ApiConf
var client *gorequest.SuperAgent

func init() {
    wg = sync.WaitGroup{}
    Meta = json.New()
}

func SetApi(url, bk_user string, supplier_id int, mongo string) {

    apiconf = &ApiConf{url, bk_user, supplier_id, mongo}

    client = newHttpClient()
}

func newHttpClient() (client *gorequest.SuperAgent) {
    client = gorequest.New()
    client.SetDoNotClearSuperAgent(true)
    client.Set("Content-Type", "application/json")
    client.Set("BK_USER", apiconf.BK_USER)
    client.Set("HTTP_BLUEKING_SUPPLIER_ID", Str(apiconf.SUPPLIER_ID))
    return
}

func ShowMeta() {
    fmt.Println(Meta.ToStringPretty())
}

func (o *Orm) Show() {
    fmt.Println(o.ToStringPretty())
}

func (o *Orm) ShowConf() {
    fmt.Println(o.conf.ToStringPretty())
}

func NewOrm() (o *Orm) {
	o = &Orm{
		json.FromString("{}"),
		json.New(),
	}
	return
}

func (o *Orm) Create(obj interface{}) *Orm {
    /* 
        This method just set the foreign key field to its appropriage type.
        ToOne field would be set to the foreign object's bk_inst_id.
        ToMany field would be set to a string that joined by all foreign 
        objects' bk_inst_id and a comma.
    */
    o.Json = json.FromStruct(obj)

    o.Del("base")

    o.conf.Set("C_obj_pointer", mustPointer(obj))
	o.conf.Set("ObjId", bkObjIdOf(obj))
	o.conf.Set("Method", POST)
	o.conf.Set("Operation", C)

	o.conf.EnSet(json.FromString("[]"), "Temp", "Asst")

    // current instance's id, we'll change it to the real id in "updateAsst"
    inst_id := 0    
	o.conf.Set("C_InstId", inst_id)

    for _, field_name := range getForeignKeys(obj, "toMany") {
        field_data := o.Get(field_name)

        obj_asst_id := Join("_", o.conf.GetString("ObjId"), 
                              "default", field_name)

        for i := 0; i < len(field_data.MustArray()); i++ {
            asst_inst_data := field_data.GetIndex(i)
            
            asst_inst_id := asst_inst_data.GetInt("bk_inst_id")
            if asst_inst_id != 0 {
                asst_create_data := map[string]interface{}{
                                        "bk_obj_asst_id": obj_asst_id,
                                        "bk_inst_id":inst_id,
                                        "bk_asst_inst_id":asst_inst_id,
                                    }
                o.conf.GetPath("Temp").Append("Asst", asst_create_data)
            }
        }
        o.Del(field_name)
    }

    for _, field_name := range getForeignKeys(obj, "toOne") {
        field_data := o.Get(field_name)
        asst_inst_id := field_data.GetInt("bk_inst_id")

        obj_asst_id := Join("_", o.conf.GetString("ObjId"), 
                              "default", field_name)

        if asst_inst_id != 0 {
            asst_create_data := map[string]interface{}{
                                    "bk_obj_asst_id": obj_asst_id,
                                    "bk_inst_id":inst_id,
                                    "bk_asst_inst_id":asst_inst_id,
                                }
            o.conf.GetPath("Temp").Append("Asst", asst_create_data)
        }
        o.Del(field_name)
    }

    //o.Show()
    return o
}

func (o *Orm) ReadAsst() *Orm {
	o.conf.Set("R_ReadAsst", true)
    return o
}

func (o *Orm) ReadRef() *Orm {
	o.conf.Set("R_ReadRef", true)
    return o
}

func (o *Orm) Read(obj interface{}) *Orm {
    bk_obj_id := bkObjIdOf(obj)

	o.EnSet(bk_obj_id, "condition", "bk_obj_id")

	o.conf.Set("ObjId", bk_obj_id)
	o.conf.Set("Method", POST)
	o.conf.Set("Operation", R)
    o.conf.Set("R_obj_pointer", mustPointer(obj))
    return o
}

func (o *Orm) Update(obj interface{}) *Orm {
    /*
      We can use "Create" method here because create method
      does nothing but only set the foreign key fields to 
      appropriate type and add some meta data
    */
    o.Create(obj)
    o.conf.Del("C_obj_pointer")

	//o.conf.Set("ObjId", bkObjIdOf(obj))
	o.conf.Set("Method", PUT)
	o.conf.Set("Operation", U)
	o.conf.Set("U_InstId", o.GetInt("bk_inst_id"))

    return o
}

func (o *Orm) Delete(obj interface{}) *Orm {
    o.Json = json.FromStruct(obj)

	o.conf.Set("ObjId", bkObjIdOf(obj))
	o.conf.Set("D_InstId", o.GetInt("bk_inst_id"))
	o.conf.Set("Method", DELETE)

    return o
}

func (o *Orm) Page(start, limit int) *Orm {
	o.EnSet(start, "page", "start")
	o.EnSet(limit, "page", "limit")

    return o
}

func (o *Orm) Sort(sort string) *Orm {
	o.SetPath([]string{"page", "sort"}, sort)

    return o
}

func (o *Orm) Filter(field string, value interface{}) *Orm {
    // the "field" can be both struct field name and its tag name,
    // if it's struct field name, we'll change it to tag name later in "beforeDo"

	o.EnSet(value, "condition", field)

    return o
}

func (o *Orm) Field(fields ...string) *Orm {
    // we must select these fields in order to do further operations
    default_fields := []string{"bk_obj_id",
                               "bk_inst_id",
                               "bk_inst_name"}

    for _, f := range default_fields {
        if !InList(f, fields) {
            fields = append(fields, f)
        }
    }

    o.EnSet(fields, "fields")
    //o.conf.EnSet(fields, "R_Fields")
    return o
}

func (o *Orm) clearData() {
    o.Json = json.New()
}

func (o *Orm) clearConf() {
    o.conf = json.New()
}

func (o *Orm) reset() {
    //*o = *NewOrm()
    o.clearData()
    o.clearConf()
}

func mustPointer(p interface{}) interface{} {
    if reflect.TypeOf(p).Kind() != reflect.Ptr {
        panic("Need pointer to unmarshal!")
    }
    return p
}
