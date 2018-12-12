package bkorm

import (
    "fmt"
    "strings"
    "errors"

    json "github.com/jabbawockeez/go-simplejson"
    . "github.com/jabbawockeez/go-utils"
)

func (o *Orm) readRef(data *json.Json) (result_json *json.Json, err error) {
    result_json = data

    if data.Length() == 0 {
        return
    }

    objId := data.GetIndex(0).GetString("bk_obj_id")
    //refObjIds := getRefObjIds(objId)

    //instId := data.GetIndex(0).GetInt("bk_inst_id")
    //refs := getCurrentInstRef(instId, objId)
    //refs.P()

    //getAsstMapping(refs.GetIndex(0).GetString("bk_obj_asst_id"))

    for i := 0; i < data.Length(); i++ {
        inst := data.GetIndex(i)
        instId := inst.GetInt("bk_inst_id")
        inst.EnSet(json.FromString("{}"), "base", "Referer")

        refs := getCurrentInstRef(instId, objId)
        
        for j := 0; j < refs.Length(); j++ {
            item := refs.GetIndex(j)
            //getAsstMapping(item.GetString("bk_obj_asst_id"))

            idx := strings.Index(item.GetString("bk_obj_asst_id"), "_default_" + objId)
            refObjId := item.GetString("bk_obj_asst_id")[:idx]

            if _, ok := inst.GetPath("base", "Referer").CheckGet(refObjId); !ok {
                inst.EnSet(json.FromString("[]"), "base", "Referer", refObjId)
            }
            inst.GetPath("base", "Referer").Append(refObjId, item.GetInt("bk_inst_id"))
        }
        //inst.P()
    }

    return
}

func (o *Orm) readAsst(data *json.Json) (result_json *json.Json, err error) {
    //o.clearData()
    result_json = data

    insts := data
    //count := insts.Length()

    var foreignFields []string
    fieldsToGet := []string{}

    if insts.Length() == 0 {
        return
    } else {
        inst := insts.GetIndex(0)
        foreignFields = getForeignKeys(inst.GetString("bk_obj_id"), "all")

        // check if the fields are selected, if not, then we won't get its children
        //selectedFields := o.conf.GetStringArray("R_Fields")
        selectedFields := o.GetStringArray("fields")

        if len(selectedFields) == 0 {
            // if no fields are selected, then fetch all foreign fields' children
            fieldsToGet = foreignFields
        } else {
            intersec := Intersection(selectedFields, foreignFields)
            for _, f := range intersec {
                fieldsToGet = append(fieldsToGet, f.(string))
            }
        }

        if len(fieldsToGet) == 0 {
            return
        }
    }

    for i := 0; i < insts.Length(); i++ {
        inst := insts.GetIndex(i)
        inst_id := inst.GetInt("bk_inst_id")

        url := GenURL(apiconf.URL, "inst/search/topo/owner", apiconf.SUPPLIER_ID,
                      "object", o.conf.GetString("ObjId"), 
                      "inst", inst_id)

        _, body, _ := client.Post(url).End()

        result := body.GetBool("result")
        errMsg := body.GetString("bk_error_msg")
        inst_assts := body.Get("data")

        if !result {
            err = errors.New(errMsg)
            return
        }

        for i := 0; i < len(inst_assts.MustArray()); i++ {
            inst_asst := inst_assts.GetIndex(i)
            inst_asst_obj_id := inst_asst.GetString("bk_obj_id")
            
            // if the asst is not in the foreignFields list(means the asst is not 
            // definded as an embedded struct in the inst struct), then we will 
            // skip to fetch its relative instances
            if !InList(inst_asst_obj_id, fieldsToGet) {
                //P("field is not selected or not a foreign field.", inst_asst_obj_id)
                continue
            }

            children := inst_asst.Get("children").MustArray()

            for i := 0; i < len(children); i++ {
                id := children[i].(map[string]interface{})["id"].(string)
                post_data := json.FromString(`{"condition": {}}`)
                post_data.EnSet(StrToInt(id), "condition", "bk_inst_id")

                url := GenURL(apiconf.URL, "inst/search/owner",
                              apiconf.SUPPLIER_ID, "object", 
                              inst_asst.GetString("bk_obj_id"))

                _, body, _ := client.Post(url).Send(post_data.ToString()).End()

                child := body.GetPath("data", "info").GetIndex(0)

                children[i] = child.MustMap()
            }
            inst.Set(inst_asst.GetString("bk_obj_id"), children)
        }
    } // for
    result_json = data

    return
}

// find out keys in "a" but not in "b" and set them into resultA,
// and find out keys in "b" but not in "a" and set them into resultB
func different(a, b *json.Json) (resultA, resultB *json.Json) {
    resultA = json.FromString("[]")
    resultB = json.FromString("[]")

    aKeys := []string{}
    bKeys := []string{}

    for i := 0; i < a.Length(); i++ {
        item := a.GetIndex(i)
        key := Join("|", item.GetString("bk_obj_asst_id"),
                         item.GetInt("bk_inst_id"),
                         item.GetInt("bk_asst_inst_id"))
        aKeys = append(aKeys, key)
    }
    for i := 0; i < b.Length(); i++ {
        item := b.GetIndex(i)
        key := Join("|", item.GetString("bk_obj_asst_id"),
                         item.GetInt("bk_inst_id"),
                         item.GetInt("bk_asst_inst_id"))
        bKeys = append(bKeys, key)
    }

    da := Difference(aKeys, bKeys)
    db := Difference(bKeys, aKeys)

    for _, i := range da {
        s := strings.Split(i.(string), "|")
        item := json.FromString(fmt.Sprintf(`
                    {
                        "bk_obj_asst_id": "%s",
                        "bk_inst_id": %d,
                        "bk_asst_inst_id": %d
                     }`, 
                     s[0], StrToInt(s[1]), StrToInt(s[2])))
        resultA.Append("", item)
    }
    for _, i := range db {
        s := strings.Split(i.(string), "|")
        item := json.FromString(fmt.Sprintf(`
                    {
                        "bk_obj_asst_id": "%s",
                        "bk_inst_id": %d,
                        "bk_asst_inst_id": %d
                     }`, 
                     s[0], StrToInt(s[1]), StrToInt(s[2])))
        resultB.Append("", item)
    }

    return
}

func (o *Orm) updateAsst() (err error) {
    createUrl := GenURL(apiconf.URL, "inst/association/action/create")
    //deleteUrl := GenURL(apiconf.URL, "inst/association/action/delete")

    asst := o.conf.GetPath("Temp", "Asst")

    instId := o.conf.GetInt("U_InstId")

    currInstAsst := getCurrentInstAsst(o.conf.GetInt("U_InstId"))

    for i := 0; i < asst.Length(); i++ {
        asst_data := asst.GetIndex(i)
        asst_data.EnSet(instId, "bk_inst_id")
    }

    asstToCreate, asstToDelete := different(asst, currInstAsst)

    // first remove all asst that should be removed,
    // because if it is a toOne asst, we can't just update it,
    // so we remove it first and add it later
    for i := 0; i < asstToDelete.Length(); i++ {
        asst_data := asstToDelete.GetIndex(i)

        /*
          we should use the item in "asstToDelete" to find the item in the 
          "currInstAsst" because item in "asstToDelete" only contains three basic
          fields(bk_obj_asst_id, bk_inst_id, bk_asst_inst_id) but item in 
          "currInstAsst" contains the other three fields(_id, _created, _modified),
          which is used by bongo to delete the exact document
        */
        for i := 0; i < currInstAsst.Length(); i++ {
            item := currInstAsst.GetIndex(i)
            if (asst_data.GetString("bk_obj_asst_id") == 
                item.GetString("bk_obj_asst_id")) && 
               (asst_data.GetInt("bk_inst_id") == 
                item.GetInt("bk_inst_id")) && 
               (asst_data.GetInt("bk_asst_inst_id") == 
                item.GetInt("bk_asst_inst_id")) {
                asst_data = item
                break
            }
        }

        // ==================================================
        // below is delete the asst by sending data to cmdb api, 
        // but now it still has bug, so we delete it from mongo directly
        //_, body, _ := client.Post(createUrl).Send(asst_data.ToString()).End()

        //result := body.GetBool("result")
        //errMsg := body.GetString("bk_error_msg")

        //if !result {
        //    err = errors.New(errMsg)
        //    return
        //}
        // ==================================================

        err = deleteFromMongo(asst_data)
        if err != nil {
            return
        }
    }

    for i := 0; i < asstToCreate.Length(); i++ {
        asst_data := asstToCreate.GetIndex(i)

        _, body, _ := client.Post(createUrl).Send(asst_data.ToString()).End()

        result := body.GetBool("result")
        errMsg := body.GetString("bk_error_msg")

        if !result {
            err = errors.New(errMsg)
            return
        }
    }

    //o.conf.Get("Temp").Del("Asst")
    return
}

func (o *Orm) createAsst() (err error) {
    return
}

func (o *Orm) deleteAsst() (err error) {
    return
}
