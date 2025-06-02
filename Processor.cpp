#include "Processor.h"
#include "globe.h"
#include "LogComm.h"
#include "DataProxyProto.h"
#include "ServiceDefine.h"
#include "util/tc_hash_fun.h"
#include "uuid.h"
#include "CommonStruct.pb.h"
#include "CommonCode.pb.h"
#include "ClubProto.h"
#include "GlobalServer.h"
#include "Java2RoomProto.h"
#include "RoomServant.h"
#include "third.pb.h"


//
using namespace std;
using namespace dataproxy;
using namespace dbagent;

//
#define GIFT_EXPIRED_TIME (24*60*60)

/**
 *
*/
Processor::Processor()
{

}

/**
 *
*/
Processor::~Processor()
{

}

int Processor::readDataFromDBEx(long uid, const string& table_name, const std::vector<string>& col_name, const std::vector<vector<string>>& whlist, const string& order_col, dbagent::TDBReadRsp &dataRsp)
{
    int iRet = 0;
    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = table_name;

    vector<dbagent::TField> fields;
    dbagent::TField tfield;
    tfield.colArithType = E_NONE;
    for(auto item : col_name)
    {
        tfield.colName = item;
        fields.push_back(tfield);
    }
    rDataReq.fields = fields;

    //where条件组
    if(!whlist.empty())
    {
        vector<dbagent::ConditionGroup> conditionGroups;
        dbagent::ConditionGroup conditionGroup;
        conditionGroup.relation = dbagent::AND;
        vector<dbagent::Condition> conditions;
        for(auto item : whlist)
        {
            if(item.size() != 3)
            {
                continue;
            }
            dbagent::Condition condition;
            condition.condtion =  dbagent::Eum_Condition(S2I(item[1]));
            condition.colType = dbagent::STRING;
            condition.colName = item[0];
            condition.colValues = item[2];
            conditions.push_back(condition);
        }
        conditionGroup.condition = conditions;
        conditionGroups.push_back(conditionGroup);
        rDataReq.conditions = conditionGroups;
    }

    //order by字段
    if(!order_col.empty())
    {
        vector<dbagent::OrderBy> orderBys;
        dbagent::OrderBy orderBy;
        orderBy.sort = dbagent::DESC;
        orderBy.colName = order_col;
        orderBys.push_back(orderBy);
        rDataReq.orderbyCol = orderBys;
    }
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->read(rDataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ",dataRsp: " << printTars(dataRsp) << endl;
        return -1;
    }
    return 0;
}

int Processor::readDataFromDB(long uid, const string& table_name, const std::vector<string>& col_name, const map<string, string>& whlist, const string& order_col, int limit_num,  dbagent::TDBReadRsp &dataRsp)
{
    int iRet = 0;
    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = table_name;

    vector<dbagent::TField> fields;
    dbagent::TField tfield;
    tfield.colArithType = E_NONE;
    for(auto item : col_name)
    {
        tfield.colName = item;
        fields.push_back(tfield);
    }
    rDataReq.fields = fields;

    //where条件组
    if(!whlist.empty())
    {
        vector<dbagent::ConditionGroup> conditionGroups;
        dbagent::ConditionGroup conditionGroup;
        conditionGroup.relation = dbagent::AND;
        vector<dbagent::Condition> conditions;
        for(auto item : whlist)
        {
            dbagent::Condition condition;
            condition.condtion = dbagent::E_EQ;
            condition.colType = dbagent::STRING;
            condition.colName = item.first;
            condition.colValues = item.second;
            conditions.push_back(condition);
        }
        conditionGroup.condition = conditions;
        conditionGroups.push_back(conditionGroup);
        rDataReq.conditions = conditionGroups;
    }

    //order by字段
    if(!order_col.empty())
    {
        vector<dbagent::OrderBy> orderBys;
        dbagent::OrderBy orderBy;
        orderBy.sort = dbagent::DESC;
        orderBy.colName = order_col;
        orderBys.push_back(orderBy);
        rDataReq.orderbyCol = orderBys;
    }

    if(limit_num > 0)
    {
        //指定返回的行数的最大值
        rDataReq.limit = limit_num;
        //指定返回的第一行的偏移量
        rDataReq.limit_from = 0;
    }

    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->read(rDataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ",dataRsp: " << printTars(dataRsp) << endl;
        return -1;
    }
    return 0;
}

int Processor::writeDataFromDB(dbagent::Eum_Query_Type dBOPType, long uid, const string& table_name, const std::map<string, string>& col_info, const map<string, string>& whlist)
{
    int iRet = 0;
    //更新上个赛季状态
    dbagent::TDBWriteReq uDataReq;
    uDataReq.keyIndex = 0;
    uDataReq.queryType = dBOPType;
    uDataReq.tableName = table_name;

    vector<dbagent::TField> fields;
    dbagent::TField tfield;

    for(auto item : col_info)
    {
        tfield.colArithType = E_NONE;
        tfield.colType = dbagent::STRING;
        tfield.colName = item.first;
        tfield.colValue = item.second;
        fields.push_back(tfield);
    }
    uDataReq.fields = fields;

    //where条件组
    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;
    dbagent::Condition condition;
    for(auto item : whlist)
    {
        dbagent::Condition condition;
        condition.condtion = dbagent::E_EQ;
        condition.colType = dbagent::STRING;
        condition.colName = item.first;
        condition.colValues = item.second;
        conditions.push_back(condition);
    }
    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    uDataReq.conditions = conditionGroups;

    dbagent::TDBWriteRsp uDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->write(uDataReq, uDataRsp);
    if (iRet != 0 || uDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "update data to dbagent failed, uDataReq:" << printTars(uDataReq) << ",uDataRsp: " << printTars(uDataRsp) << endl;
        return -1;
    }
    return 0;
}

int Processor::delDataFromDB(long uid, const string& table_name, const map<string, string>& whlist)
{
    int iRet = 0;
    dbagent::TDBWriteReq uDataReq;
    uDataReq.keyIndex = 0;
    uDataReq.queryType = dbagent::E_DELETE;
    uDataReq.tableName = table_name;

    vector<dbagent::TField> fields;
    dbagent::TField tfield;

    //where条件组
    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;
    dbagent::Condition condition;
    for(auto item : whlist)
    {
        dbagent::Condition condition;
        condition.condtion = dbagent::E_EQ;
        condition.colType = dbagent::STRING;
        condition.colName = item.first;
        condition.colValues = item.second;
        conditions.push_back(condition);
    }
    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    uDataReq.conditions = conditionGroups;

    dbagent::TDBWriteRsp uDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->write(uDataReq, uDataRsp);
    if (iRet != 0 || uDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "update data to dbagent failed, uDataReq:" << printTars(uDataReq) << ",uDataRsp: " << printTars(uDataRsp) << endl;
        return -1;
    }
    return 0;
}

//查询消息
int Processor::queryMessages(const long lUid, const GlobalProto::TQueryMessageReq &req, GlobalProto::TQueryMessageResp& resp)
{
    int iRet = 0;
    FUNC_ENTRY("");

    string table_name = "tb_message";
    std::vector<string> col_name = {"message_id", "message_state", "message_date", "message_content"};
    std::vector<vector<string>> whlist = {
        {"message_type", "0", I2S(int(req.imailtype()))},
        {"message_del", "0", "0"}
    };
    if( global::E_MESSAGE_TYPE(req.imailtype()) != global::E_MESSAGE_TYPE::E_MESSAGE_NOTICE)
    {
        whlist.push_back({"uid", "0", L2S(lUid)});
        whlist.push_back({"message_state", req.itype() == 0 ? "4" : "0", "0"});
    }
    else
    {
        whlist.push_back({"message_date", "4", g_app.getOuterFactoryPtr()->GetTimeFormatOffSet(60)});
    }
    dbagent::TDBReadRsp dataRsp;
    iRet = readDataFromDBEx(lUid, table_name, col_name, whlist, "message_date", dataRsp);
    if(iRet != 0)
    {
        LOG_ERROR<<"query message err! uid: "<< lUid << endl;
        return iRet;
    }

    if(req.icurrentpage() <= 0)
    {
        LOG_ERROR << "param err. icurrentpage: "<< req.icurrentpage() << endl;
        return -1;
    }

    int pageCount = 10;
    int startIndex = (req.icurrentpage() -1) * pageCount;

    LOG_DEBUG << "startIndex: "<< startIndex << ", page: "<< req.icurrentpage() << ", size: "<< dataRsp.records.size() << endl;

    int i = 0;
    int count = 0;
    for (auto it = dataRsp.records.begin(); it != dataRsp.records.end(); ++it)
    {
        i++;
        if(i < startIndex)
        {
            continue;
        }
        if(count >= pageCount)
        {
            break;
        }

        long message_id = 0;
        int message_state = 0;
        string message_date;
        string message_content;
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            if (itfield->colName == "message_id")
            {
                message_id = S2L(itfield->colValue);
            }
            if (itfield->colName == "message_state")
            {
                message_state = S2I(itfield->colValue);
            }
            if (itfield->colName == "message_date")
            {
                message_date = itfield->colValue;
            }
            if (itfield->colName == "message_content")
            {
                message_content = itfield->colValue;
            }
        }

        GlobalProto::TMessageData_Info tInfo;
        tInfo.ParseFromString(message_content);
        if(!req.sroomkey().empty() && tInfo.sroomkey() != req.sroomkey())
        {
           continue;
        }

        auto info = resp.add_messages();
        info->set_imailtype(req.imailtype());
        info->set_lmessageid(message_id);
        info->set_istate(message_state);
        info->set_messagedate(message_date);
        (*info->mutable_sinfo()).ParseFromString(message_content);
        count++;
    }
    int pages = dataRsp.records.size() / pageCount != 0 && dataRsp.records.size() > 0 ? dataRsp.records.size() / pageCount + 1 : dataRsp.records.size() / pageCount;
    resp.set_totalpage(pages);
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::insertMessage(const global::MessageReq &req, bool bSysNotice)
{
    int iRet = 0;
    FUNC_ENTRY("");

    bSysNotice = req.iMailType == global::E_MESSAGE_TYPE::E_MESSAGE_NOTICE;

    userinfo::GetUserBasicReq userInfoReq;
    userInfoReq.uid = req.sPresidentID;
    userinfo::GetUserBasicResp userInfoResp;
    iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(userInfoReq.uid)->getUserBasic(userInfoReq, userInfoResp);
    if (iRet != 0 && !bSysNotice)
    {
        ROLLLOG_ERROR << "getUserBasic failed, uid: " << userInfoReq.uid << endl;
        return -1;
    }

    userinfo::GetUserBasicReq preUserInfoReq;
    preUserInfoReq.uid = req.lPlayerID;
    userinfo::GetUserBasicResp preUserInfoResp;
    iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(preUserInfoReq.uid)->getUserBasic(preUserInfoReq, preUserInfoResp);
    if (iRet != 0 && !bSysNotice)
    {
        ROLLLOG_ERROR << "getUserBasic failed, uid: " << preUserInfoReq.uid << endl;
        return -1;
    }

    GlobalProto::TMessageData_Info info;
    info.set_lplayerid(req.sPresidentID);
    info.set_snickname(userInfoResp.userinfo.name);
    info.set_sheadstr(userInfoResp.userinfo.head);
    info.set_iplayergender(userInfoResp.userinfo.gender);
    info.set_stitle(req.sTitle);
    info.set_scontent(req.sContent);
    info.set_lamount(req.lAmount);
    info.set_lclubid(req.lClubID);
    info.set_spresidentname(preUserInfoResp.userinfo.name);
    info.set_sclubname(req.sClubName);
    info.set_igametype(req.iGameType);
    info.set_sroomkey(req.sRoomKey);
    info.set_sroomid(req.sRoomID);
    info.set_sgamename(req.sGameName);
    info.set_icontenttype (req.iContentType);
    info.set_lpresidentid (req.sPresidentID);
    info.set_ltime (req.lTime);

    string s_info;
    info.SerializeToString(&s_info);

    string table_name = "tb_message";
    std::map<string, string> col_info = {{"uid", L2S(req.lPlayerID)}, {"message_type", I2S(int(req.iMailType))},
                {"message_content", s_info}, {"message_date", g_app.getOuterFactoryPtr()->GetTimeFormat()}, {"message_index", req.sMessageIndex},
    };
    iRet = writeDataFromDB(dbagent::E_INSERT, req.lPlayerID, table_name, col_info, {});
    if(iRet != 0)
    {
        LOG_ERROR << "write into db err. iRet: "<< iRet << endl;
        return iRet;
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

//处理消息
int Processor::dealMessages(const long messageId, const int iStatus)
{
    string table_name = "tb_message";
    std::vector<string> col_name = {"uid", "message_type", "message_state", "message_content"};
    std::map<string, string> whlist = {
            {"message_id", L2S(messageId)},
            {"message_del", "0"},
        };
    dbagent::TDBReadRsp dataRsp;
    int iRet = readDataFromDB(0, table_name, col_name, whlist, "", 0, dataRsp);
    if(iRet != 0)
    {
        LOG_ERROR<<"query message err! messageId: "<< messageId << endl;
        return iRet;
    }

    int message_type = 0;
    int message_state = -1;
    long lPlayerID = 0;
    string message_content;

    LOG_DEBUG << "dataRsp: "<< printTars(dataRsp)<< endl;
    for (auto it = dataRsp.records.begin(); it != dataRsp.records.end(); ++it)
    {
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            if (itfield->colName == "uid")
            {
                lPlayerID = S2L(itfield->colValue);
            }
            if (itfield->colName == "message_content")
            {
                message_content = itfield->colValue;
            }
            if (itfield->colName == "message_type")
            {
                message_type = S2I(itfield->colValue);
            }
            if (itfield->colName == "message_state")
            {
                message_state = S2I(itfield->colValue);
            }
        }
    }

    if(message_state != 0)
    {
        return message_state > 0 ? XGameRetCode::MESSAGE_ALREAD_DEAL :  XGameRetCode::MESSAGE_NOT_EXIT;
    }

    if(message_type == 0 || message_content.empty())
    {
        LOG_ERROR<<"query message err! messageId: "<< messageId << endl;
        return -1;
    }

    GlobalProto::TMessageData_Info content;
    content.ParseFromString(message_content);
    if(GlobalProto::E_MESSAGE_TYPE(message_type) == GlobalProto::E_MESSAGE_TYPE::E_MESSAGE_CLUB_GOLD)
    {
        LOG_DEBUG << "deal message test... messageId: "<< messageId << endl;
        auto pServantPrx = g_app.getOuterFactoryPtr()->getSocialServantPrx(content.lplayerid());
        if(!pServantPrx)
        {
            LOG_ERROR << "pServantPrx is nullptr."<< endl;
            return -2;
        }

        Club::InnerClubAuditApplyBalanceReq req;
        req.uId = lPlayerID;
        req.cId = content.lclubid();
        req.agree = iStatus == 1 ? true : false;
        req.targetUid = content.lpresidentid();
        req.iType = content.icontenttype();
        req.amount = content.lamount();

        Club::InnerClubAuditApplyBalanceResp resp;
        iRet = pServantPrx->InnerClubAuditApplyBalance(req, resp);
        if(iRet != 0)
        {
            LOG_ERROR << "deal message err. messageId: "<< messageId << endl;
            return iRet;
        }
    }
    else if (GlobalProto::E_MESSAGE_TYPE(message_type) == GlobalProto::E_MESSAGE_TYPE::E_MESSAGE_JOIN_GAME)
    {
        LOG_DEBUG << "deal message test... messageId: "<< messageId << ", sRoomID: "<< content.sroomid() << endl;
        auto pServantPrx = g_app.getOuterFactoryPtr()->getRoomServantPrx(content.lplayerid(), content.sroomid());
        if(!pServantPrx)
        {
            LOG_ERROR << "pServantPrx is nullptr."<< endl;
            return -2;
        }

        java2room::RoomAuditApplyReq req;
        req.roomKey = content.sroomkey();
        req.uId = content.lpresidentid();
        req.sRoomID = content.sroomid();
        req.bAgree = iStatus == 1 ? true : false;
                
        iRet = pServantPrx->onRoomAuditApply(req);
        if(iRet != 0)
        {
            LOG_ERROR << "deal message err. messageId: "<< messageId << endl;
            return iRet;
        }
    }
    return 0;
}

//更新消息
int Processor::updateMessages(const long lUid, const map<string, string>& updateData, const map<string, string>& whlData)
{
    int iRet = 0;
    FUNC_ENTRY("");

    string table_name = "tb_message";
    iRet = writeDataFromDB(dbagent::E_UPDATE, lUid, table_name, updateData, whlData);
    if(iRet != 0)
    {
        LOG_ERROR << "write into db err. iRet: "<< iRet << endl;
        return iRet;
    }
    FUNC_EXIT("", iRet);
    return iRet;
}

//替换消息
int Processor::replaceRedDot(const long lUid, const int iFlag, const long lExtend)
{
    int iRet = 0;

    FUNC_ENTRY("");

    dbagent::TDBWriteReq uDataReq;
    uDataReq.keyIndex = 0;
    uDataReq.queryType = dbagent::E_REPLACE;
    uDataReq.tableName = "tb_red_dot";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colType = dbagent::BIGINT;
    tField.colName = "uid";
    tField.colValue = L2S(lUid);
    fields.push_back(tField);

    tField.colType = dbagent::INT;
    tField.colName = "flag";
    tField.colValue = I2S(iFlag);
    fields.push_back(tField);

    tField.colType = dbagent::BIGINT;
    tField.colName = "extend";
    tField.colValue = L2S(lExtend);
    fields.push_back(tField);

    tField.colType = dbagent::INT;
    tField.colName = "read_date";
    tField.colValue = I2S(TNOW);
    fields.push_back(tField);

    uDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;
    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "uid";
    condition.colValues = L2S(lUid);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "flag";
    condition.colValues = I2S(iFlag);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "extend";
    condition.colValues = L2S(lExtend);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    uDataReq.conditions = conditionGroups;

    dbagent::TDBWriteRsp uDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->write(uDataReq, uDataRsp);
    if (iRet != 0 || uDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "update data to dbagent failed, uDataReq:" << printTars(uDataReq) << ", uDataRsp:" << printTars(uDataRsp) << endl;
        return -1;
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

//查询红点
int Processor::queryRedDot(const long lUid, const int iFlag, vector<global::RedDotInfo> &infos)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_red_dot";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "flag";
    fields.push_back(tField);

    tField.colName = "extend";
    fields.push_back(tField);

    tField.colName = "read_date";
    fields.push_back(tField);

    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "uid";
    condition.colValues = L2S(lUid);
    conditions.push_back(condition);

    if (iFlag == 0)
    {
        condition.condtion = dbagent::E_IN;
        condition.colType = dbagent::INT;
        condition.colName = "flag";
        condition.colValues = "(1,2,3,4,5,6,20,30)";
        conditions.push_back(condition);
    }
    else if (iFlag == 1)
    {
        condition.condtion = dbagent::E_IN;
        condition.colType = dbagent::INT;
        condition.colName = "flag";
        condition.colValues = "(1,2,3,4,5,6,30)";
        conditions.push_back(condition);
    }
    else if (iFlag == 2)
    {
        condition.condtion = dbagent::E_EQ;
        condition.colType = dbagent::INT;
        condition.colName = "flag";
        condition.colValues = L2S(10);
        conditions.push_back(condition);
    }
    else if (iFlag == 4)
    {
        condition.condtion = dbagent::E_EQ;
        condition.colType = dbagent::INT;
        condition.colName = "flag";
        condition.colValues = L2S(20);
        conditions.push_back(condition);
    }
    else
    {
        condition.condtion = dbagent::E_EQ;
        condition.colType = dbagent::INT;
        condition.colName = "flag";
        condition.colValues = L2S(30);
        conditions.push_back(condition);
    }

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", rDataRsp: "<< printTars(rDataRsp) << endl;

    global::RedDotInfo info;
    for (auto it = rDataRsp.records.begin(); it != rDataRsp.records.end(); ++it)
    {
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            if (itfield->colName == "flag")
            {
                info.iFlag = S2I(itfield->colValue);
            }
            else if (itfield->colName == "extend")
            {
                info.lExtend = S2L(itfield->colValue);
            }
            else if (itfield->colName == "read_date")
            {
                info.iRead = S2I(itfield->colValue);
            }
        }
        infos.push_back(info);
    }
    
    FUNC_EXIT("", iRet);
    return iRet;
}

//查询信息
int Processor::queryMessagesForRedDot(const long lUid, const int iType, const int iRead, int &count)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_message";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "uid";
    fields.push_back(tField);
    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "uid";
    condition.colValues = iType == 1 ? L2S(0) : L2S(lUid);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "message_type";
    condition.colValues = I2S(iType);
    conditions.push_back(condition);

    string sFormat("%Y-%m-%d %H:%M:%S");
    condition.condtion = dbagent::E_GE;
    condition.colType = dbagent::STRING;
    condition.colName = "message_date";
    condition.colValues = g_app.getOuterFactoryPtr()->timeFormat(sFormat, iRead);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", rDataRsp: "<< printTars(rDataRsp) << endl;
    count = rDataRsp.totalcount;
    
    FUNC_EXIT("", iRet);
    return iRet;
}

// 查询俱乐部ID
int Processor::queryClubId(const long lUid, vector<long> &vClubIds)
{
    int iRet = 0;

    FUNC_ENTRY("");
    
    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_club_info";

    vector<dbagent::TField> fields;
    dbagent::TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "cid";
    fields.push_back(tfield);

    rDataReq.fields = fields;

    //where条件组
    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;
    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "status";
    condition.colValues = I2S(0);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "uid";
    condition.colValues = L2S(lUid);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq: " << printTars(rDataReq) << ", rDataRsp: " << printTars(rDataRsp) << endl;
        return iRet;
    }

    for (auto it = rDataRsp.records.begin(); it != rDataRsp.records.end(); ++it)
    {
        for (auto itField = it->begin(); itField != it->end(); ++itField)
        {
            if (itField->colName == "cid")
            {
                vClubIds.push_back(S2L(itField->colValue));
                break;
            }
        }
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

//查询俱乐部申请信息
int Processor::queryClubApplyForRedDot(const long lUid, const long lCid, const int iRead, int &count)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_club_apply";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "uid";
    fields.push_back(tField);
    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "cid";
    condition.colValues = L2S(lCid);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "status";
    condition.colValues = I2S(0);
    conditions.push_back(condition);

    string sFormat("%Y-%m-%d %H:%M:%S");
    condition.condtion = dbagent::E_GE;
    condition.colType = dbagent::STRING;
    condition.colName = "applied";
    condition.colValues = g_app.getOuterFactoryPtr()->timeFormat(sFormat, iRead);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", cid:" << lCid << ", rDataRsp: "<< printTars(rDataRsp) << endl;
    count = rDataRsp.totalcount;
    
    FUNC_EXIT("", iRet);
    return iRet;
}

//查询好友申请信息
int Processor::queryFriendApplyForRedDot(const long lUid, const int iRead, int &count)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_friend";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "uid";
    fields.push_back(tField);
    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::BIGINT;
    condition.colName = "uid";
    condition.colValues = L2S(lUid);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "relationship";
    condition.colValues = I2S(1);
    conditions.push_back(condition);

    string sFormat("%Y-%m-%d %H:%M:%S");
    condition.condtion = dbagent::E_GE;
    condition.colType = dbagent::STRING;
    condition.colName = "log_time";
    condition.colValues = g_app.getOuterFactoryPtr()->timeFormat(sFormat, iRead);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", rDataRsp: "<< printTars(rDataRsp) << endl;
    count = rDataRsp.totalcount;
    
    FUNC_EXIT("", iRet);
    return iRet;
}

// 查询联盟ID
int Processor::queryUnionId(const long lUid, vector<long> &vUnionIds)
{
    int iRet = 0;

    FUNC_ENTRY("");
    
    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_union";

    vector<dbagent::TField> fields;
    dbagent::TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "nid";
    fields.push_back(tfield);

    rDataReq.fields = fields;

    //where条件组
    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;
    dbagent::Condition condition;
    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "status";
    condition.colValues = I2S(0);
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "uid";
    condition.colValues = L2S(lUid);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq: " << printTars(rDataReq) << ", rDataRsp: " << printTars(rDataRsp) << endl;
        return iRet;
    }

    for (auto it = rDataRsp.records.begin(); it != rDataRsp.records.end(); ++it)
    {
        for (auto itField = it->begin(); itField != it->end(); ++itField)
        {
            if (itField->colName == "nid")
            {
                vUnionIds.push_back(S2L(itField->colValue));
                break;
            }
        }
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

//查询联盟申请信息
int Processor::queryUnionApplyForRedDot(const long lUid, const std::string sNid, const int iRead, int &count)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_union_apply";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "cid";
    fields.push_back(tField);
    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_IN;
    condition.colType = dbagent::BIGINT;
    condition.colName = "nid";
    condition.colValues = sNid;
    conditions.push_back(condition);

    condition.condtion = dbagent::E_EQ;
    condition.colType = dbagent::INT;
    condition.colName = "status";
    condition.colValues = I2S(0);
    conditions.push_back(condition);

    string sFormat("%Y-%m-%d %H:%M:%S");
    condition.condtion = dbagent::E_GE;
    condition.colType = dbagent::STRING;
    condition.colName = "applied";
    condition.colValues = g_app.getOuterFactoryPtr()->timeFormat(sFormat, iRead);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", sNid:" << sNid << ", rDataRsp: "<< printTars(rDataRsp) << endl;
    count = rDataRsp.totalcount;
    
    FUNC_EXIT("", iRet);
    return iRet;
}

// 查询活动信息
int Processor::queryActvitysForRedDot(const long lUid, const int iRead, int &count)
{
    int iRet = 0;
    FUNC_ENTRY("");

    dbagent::TDBReadReq rDataReq;
    rDataReq.keyIndex = 0;
    rDataReq.queryType = dbagent::E_SELECT;
    rDataReq.tableName = "tb_sys_activity_msg";

    vector<dbagent::TField> fields;
    dbagent::TField tField;
    tField.colArithType = E_NONE;
    tField.colName = "id";
    fields.push_back(tField);
    rDataReq.fields = fields;

    vector<dbagent::ConditionGroup> conditionGroups;
    dbagent::ConditionGroup conditionGroup;
    conditionGroup.relation = dbagent::AND;
    vector<dbagent::Condition> conditions;

    dbagent::Condition condition;
    condition.condtion = dbagent::E_GE;
    condition.colType = dbagent::INT;
    condition.colName = "create_time";
    condition.colValues = I2S(iRead);
    conditions.push_back(condition);

    conditionGroup.condition = conditions;
    conditionGroups.push_back(conditionGroup);
    rDataReq.conditions = conditionGroups;

    dbagent::TDBReadRsp rDataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->read(rDataReq, rDataRsp);
    if (iRet != 0 || rDataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "read data from dbagent failed, rDataReq:" << printTars(rDataReq) << ", rDataRsp:" << printTars(rDataRsp) << endl;
        return -1;
    }


    LOG_DEBUG << "uid:" << lUid << ", rDataRsp: "<< printTars(rDataRsp) << endl;
    count = rDataRsp.totalcount;
    
    FUNC_EXIT("", iRet);
    return iRet;
}
/***********************好友模块**********************/

//检查好友关系
bool Processor::checkFriendRelation(tars::Int64 uid, tars::Int64 friend_uid, FriendsProto::Eum_Friend_Relationship_Type relationship)
{
    FUNC_ENTRY("");

    dataproxy::TReadDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(FRIEND_INFO) + ":" + L2S(uid);
    dataReq.operateType = E_REDIS_READ;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = uid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.subOperateType = E_REDIS_LIST_RANGE;//根据范围取数据
    dataReq.paraExt.start = 0;//起始下标从0开始
    dataReq.paraExt.end = -1;//终止最大结束下标为-1

    vector<TField> fields; //TODO 为什么只传入一个字段,仍然把全部字段都读出来了
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "friend_uid";
    tfield.colType = BIGINT;
    fields.push_back(tfield);

    TReadDataRsp dataRsp;
    int iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->redisRead(dataReq, dataRsp);
    if(iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_DEBUG << "checkFriendRelation failed!" << endl;
        return false;
    }

    vector<tars::Int32> friendList;
    for (auto it = dataRsp.fields.begin(); it != dataRsp.fields.end(); ++it)
    {
        bool isFriend = false;
        tars::Int64 friend_uid = -1;
        for (auto itTField = it->begin(); itTField != it->end(); ++itTField)
        {
            if (itTField->colName == "friend_uid")
                friend_uid = S2L(itTField->colValue);

            if (itTField->colName == "relationship")
                isFriend = S2I(itTField->colValue) == relationship ? true : false;
        }

        if (isFriend && friend_uid != -1)
        {
            friendList.push_back(friend_uid);
        }
    }

    auto it = find(friendList.begin(), friendList.end(), friend_uid);
    if (it != friendList.end())
    {
        return  true;
    }

    FUNC_EXIT("", 0);

    return false;
}

//TODO 插入好友
int Processor::InsertFriendEntry(tars::Int64 uid, tars::Int64 friend_uid, std::string content, FriendsProto::Eum_Friend_Relationship_Type relationship)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    dataproxy::TWriteDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(FRIEND_INFO) + ":" + L2S(uid);
    dataReq.operateType = E_REDIS_WRITE;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = uid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.queryType = E_REPLACE;

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(uid);
    fields.push_back(tfield);
    tfield.colName = "friend_uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(friend_uid);
    fields.push_back(tfield);
    tfield.colName = "relationship";
    tfield.colType = INT;
    tfield.colValue = L2S(relationship);
    fields.push_back(tfield);
    tfield.colName = "content";
    tfield.colType = STRING;
    tfield.colValue = content;
    fields.push_back(tfield);
    tfield.colName = "give_time";
    tfield.colType = STRING;
    tfield.colValue = g_app.getOuterFactoryPtr()->GetTimeFormat();
    fields.push_back(tfield);
    tfield.colName = "log_time";
    tfield.colType = STRING;
    tfield.colValue = g_app.getOuterFactoryPtr()->GetTimeFormat();
    fields.push_back(tfield);
    dataReq.fields = fields;

    dataproxy::TWriteDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->redisWrite(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "insert friend entry err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return -2;
    }

    ROLLLOG_DEBUG << "insert friend entry, iRet: " << iRet << ", dataRsp: " << printTars(dataRsp) << endl;

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int Processor::UpdateFriendEntry(tars::Int64 uid, tars::Int64 friend_uid, FriendsProto::Eum_Friend_Relationship_Type relationship)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    dataproxy::TWriteDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(FRIEND_INFO) + ":" + L2S(uid);
    dataReq.operateType = E_REDIS_WRITE;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = uid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.queryType = E_REPLACE;

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(uid);
    fields.push_back(tfield);
    tfield.colName = "friend_uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(friend_uid);
    fields.push_back(tfield);
    tfield.colName = "relationship";
    tfield.colType = INT;
    tfield.colValue = L2S(relationship);
    fields.push_back(tfield);
    tfield.colName = "give_time";
    tfield.colType = STRING;
    tfield.colValue = g_app.getOuterFactoryPtr()->GetTimeFormat();
    fields.push_back(tfield);
    dataReq.fields = fields;

    dataproxy::TWriteDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(uid)->redisWrite(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "UpdateFriendEntry entry err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return -2;
    }

    ROLLLOG_DEBUG << "UpdateFriendEntry entry, iRet: " << iRet << ", dataRsp: " << printTars(dataRsp) << endl;

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::addFriend(const long lUid, const FriendsProto::AddFriendReq &req, FriendsProto::AddFriendResp &resp)
{
    //检查是否不允许被添加好友
    userinfo::GetUserBasicReq basicReq;
    basicReq.uid = req.friend_uid();
    userinfo::GetUserBasicResp basicResp;
    int iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(basicReq.uid)->getUserBasic(basicReq, basicResp);
    if (iRet != 0)
    {
        ROLLLOG_ERROR << "getUserBasic failed, uid: " << basicReq.uid << endl;
        return -1;
    }
    if(basicResp.userinfo.banFriend == 1)
    {
        ROLLLOG_DEBUG << "checkAddFriend add friend is ban!" << endl;
        return XGameRetCode::FRIEND_ADD_FORBID;
    }

    //不能加自己
    if (lUid == req.friend_uid())
    {
        ROLLLOG_DEBUG << "can not add yourself!" << endl;
        return XGameRetCode::FRIEND_NOT_ADD_SELF;
    }

    //已是好友
    if(checkFriendRelation(lUid, req.friend_uid(), FriendsProto::E_FRIEND))
    {
        ROLLLOG_DEBUG << "has been friend relationship!" << endl;
        return XGameRetCode::FRIEND_HAS_BEEN_FRIEND;
    }

    //已经申请
    if(!checkFriendRelation(lUid, req.friend_uid(), FriendsProto::E_BE_APPLIED))
    {
        ROLLLOG_DEBUG << "has been apply friend relationship!" << endl;
        //return XGameRetCode::FRIEND_HAS_BEEN_APPLY;
    }

    iRet = InsertFriendEntry(lUid, req.friend_uid(), req.content(), FriendsProto::E_APPLICANT);
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "add friend request failed: uid : " << lUid << ", friend_uid : " << req.friend_uid() << endl;
        return -1; //
    }

    iRet = InsertFriendEntry(req.friend_uid(), lUid, req.content(), FriendsProto::E_BE_APPLIED);
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "add friend request not execute complete: uid : " << lUid << ", friend_uid : " << req.friend_uid() << endl;
        //TODO 需要删除第一条好友数据
    }
    resp.set_friend_uid(req.friend_uid());
    return iRet;
}

int Processor::deleteFriend(const long lUid, const FriendsProto::DeleteFriendReq &req, FriendsProto::DeleteFriendResp &resp)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    //发起方
    iRet = UpdateFriendEntry(lUid, req.friend_uid(), FriendsProto::E_DELETE);
    ROLLLOG_DEBUG << "DeleteFriend, iRet : " << iRet << endl;
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "Delete Friend failed : " << iRet << ", uid : " << lUid << ", friend_uid : " << req.friend_uid() << endl;
        return -1;
    }

    //接收方
    iRet = UpdateFriendEntry(req.friend_uid(), lUid, FriendsProto::E_DELETE);
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "Delete Friend incomplete : " << iRet << ", uid : " << lUid << ", friend_uid : " << req.friend_uid() << endl;
        return -2;
    }
    resp.set_friend_uid(req.friend_uid());

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::queryFriends(const long lUid, FriendsProto::Eum_Friend_Relationship_Type relationship, vector<FriendsProto::UserInfo> &vecUserInfo)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    dataproxy::TReadDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(FRIEND_INFO) + ":" + L2S(lUid);
    dataReq.operateType = E_REDIS_READ;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = lUid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.subOperateType = E_REDIS_LIST_RANGE;//根据范围取数据
    dataReq.paraExt.start = 0;//起始下标从0开始
    dataReq.paraExt.end = -1;//终止最大结束下标为-1

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "friend_uid";
    tfield.colType = BIGINT;
    fields.push_back(tfield);
    tfield.colName = "content";
    tfield.colType = STRING;
    fields.push_back(tfield);

    TReadDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->redisRead(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "get friend_uid list err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return -1;
    }

    for (auto it = dataRsp.fields.begin(); it != dataRsp.fields.end(); ++it)
    {
        map<string, string> mapRow;
        for (auto itTField = it->begin(); itTField != it->end(); ++itTField)
        {
            mapRow.insert(std::make_pair(itTField->colName, itTField->colValue));
        }
        if(S2I(mapRow["relationship"]) == relationship)
        {
            FriendsProto::UserInfo userinfo;
            userinfo.set_uid(S2L(mapRow["friend_uid"]));
            userinfo.set_apply_content(mapRow["content"]);

            userinfo::GetUserBasicReq basicReq;
            basicReq.uid = userinfo.uid();
            userinfo::GetUserBasicResp basicResp;
            iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(basicReq.uid)->getUserBasic(basicReq, basicResp);
            if (iRet != 0)
            {
                ROLLLOG_ERROR << "getUserBasic failed, uid: " << basicReq.uid << endl;
                continue;
            }
            userinfo.set_name(basicResp.userinfo.name);
            userinfo.set_head(basicResp.userinfo.head);
            userinfo.set_gender(basicResp.userinfo.gender);
            if(relationship == FriendsProto::E_FRIEND)
            {
                userinfo.set_bfriend(true);
                userinfo.set_remark_content(getUserRemark(lUid, userinfo.uid()));
                userinfo.set_signature(basicResp.userinfo.signature);
                userinfo.set_logout_time(basicResp.userinfo.lastLoginTime > basicResp.userinfo.lastLogoutTime ? 0 : TNOW - basicResp.userinfo.lastLogoutTime);
            }

            vecUserInfo.push_back(userinfo);
        }
    }
    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::agreeToAdd(const long lUid, const FriendsProto::AgreeToAddReq &req)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

     //已是好友
    if(checkFriendRelation(lUid, req.friend_uid(), FriendsProto::E_FRIEND) || checkFriendRelation(lUid, req.friend_uid(), FriendsProto::E_APPLY_BEEN_READ))
    {
        ROLLLOG_DEBUG << "has been friend relationship!" << endl;
        return XGameRetCode::FRIEND_HAS_BEEN_FRIEND;
    }

    //发起方
    FriendsProto::Eum_Friend_Relationship_Type relationship = req.is_agree() == 1 ? FriendsProto::E_FRIEND : FriendsProto::E_APPLY_BEEN_READ;
    iRet = UpdateFriendEntry(lUid, req.friend_uid(), relationship);
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "UpdateFriendEntry request failed: uid = " << lUid << ", friend_uid:" << req.friend_uid() << endl;
        return -1;
    }

    //接受方
    iRet = UpdateFriendEntry(req.friend_uid(), lUid, relationship);
    if (iRet != 0)
    {
        ROLLLOG_DEBUG << "UpdateFriendEntry request not execute complete: uid = " << lUid << ", friend_uid:" << req.friend_uid() << endl;
        return -2;
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::friendDetail(const long lUid, const long friend_uid, FriendsProto::QueryFriendDetailResp &resp)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    resp.mutable_frienddetail()->set_uid(friend_uid);

    userinfo::GetUserBasicReq basicReq;
    basicReq.uid = friend_uid;
    userinfo::GetUserBasicResp basicResp;
    iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(basicReq.uid)->getUserBasic(basicReq, basicResp);
    if (iRet != 0)
    {
        ROLLLOG_ERROR << "getUserBasic failed, uid: " << basicReq.uid << endl;
        return -1;
    }
    resp.mutable_frienddetail()->set_name(basicResp.userinfo.name);
    resp.mutable_frienddetail()->set_head(basicResp.userinfo.head);
    resp.mutable_frienddetail()->set_gender(basicResp.userinfo.gender);
    resp.mutable_frienddetail()->set_bfriend(checkFriendRelation(lUid, friend_uid, FriendsProto::E_FRIEND));
    resp.mutable_frienddetail()->set_remark_content(getUserRemark(lUid, friend_uid));
    resp.mutable_frienddetail()->set_signature(basicResp.userinfo.signature);

    //TODO  clubinfo
    auto pServantPrx = g_app.getOuterFactoryPtr()->getSocialServantPrx(lUid);
    if(!pServantPrx)
    {
        LOG_ERROR << "pServantPrx is nullptr."<< endl;
        return -2;
    }

    Club::InnerClubGetOwnSimpleReq innerClubGetOwnSimpleReq;
    innerClubGetOwnSimpleReq.uId = friend_uid;
    Club::InnerClubGetOwnSimpleResp innerClubGetOwnSimpleResp;
    iRet = pServantPrx->InnerClubGetOwnSimple(innerClubGetOwnSimpleReq, innerClubGetOwnSimpleResp);
    if(iRet != 0 || innerClubGetOwnSimpleResp.resultCode != 0)
    {
        LOG_ERROR << "get club info err. uid: "<< friend_uid << ", resultCode: "<< innerClubGetOwnSimpleResp.resultCode << endl;
        return iRet;
    }

    LOG_DEBUG << "InnerClubGetOwnSimpleResp: "<< printTars(innerClubGetOwnSimpleResp)<< endl;

    for(auto clubinfo : innerClubGetOwnSimpleResp.infoList)
    {
        auto ptr = resp.mutable_frienddetail()->add_club_info();
        ptr->set_cid(clubinfo.cId);
        ptr->set_logo(clubinfo.logo);
        ptr->set_name(clubinfo.name);
        ptr->set_country(clubinfo.country);
        ptr->set_level(clubinfo.level);
        ptr->set_position(clubinfo.position);
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::ReplaceRemark(const long lUid, tars::Int64 remark_uid, std::string content, int state)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    dataproxy::TWriteDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(USER_REMARK) + ":" + L2S(lUid);
    dataReq.operateType = E_REDIS_WRITE;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = lUid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.queryType = E_REPLACE;

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(lUid);
    fields.push_back(tfield);
    tfield.colName = "remark_uid";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(remark_uid);
    fields.push_back(tfield);
    tfield.colName = "state";
    tfield.colType = INT;
    tfield.colValue = I2S(state);
    fields.push_back(tfield);
    tfield.colName = "content";
    tfield.colType = STRING;
    tfield.colValue = content;
    fields.push_back(tfield);
    tfield.colName = "log_time";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(TNOW);
    fields.push_back(tfield);
    dataReq.fields = fields;

    dataproxy::TWriteDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->redisWrite(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "insert remark err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return -2;
    }

    ROLLLOG_DEBUG << "insert remark, iRet: " << iRet << ", dataRsp: " << printTars(dataRsp) << endl;

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

string Processor::getUserRemark(const long lUid, const long remark_uid)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

   dataproxy::TReadDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_LIST) + ":" + I2S(USER_REMARK) + ":" + L2S(lUid);
    dataReq.operateType = E_REDIS_READ;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = lUid;
    dataReq.paraExt.resetDefautlt();
    dataReq.paraExt.subOperateType = E_REDIS_LIST_RANGE;//根据范围取数据
    dataReq.paraExt.start = 0;//起始下标从0开始
    dataReq.paraExt.end = -1;//终止最大结束下标为-1

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "remark_uid";
    fields.push_back(tfield);
    tfield.colName = "state";
    fields.push_back(tfield);
    tfield.colName = "content";
    fields.push_back(tfield);
    dataReq.fields = fields;

    dataproxy::TReadDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(lUid)->redisRead(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "get user remark err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return "";
    }
    for (auto it = dataRsp.fields.begin(); it != dataRsp.fields.end(); ++it)
    {
        map<string, string> mapRow;
        for (auto itTField = it->begin(); itTField != it->end(); ++itTField)
        {
            mapRow.insert(std::make_pair(itTField->colName, itTField->colValue));
        }
        if(remark_uid == S2L(mapRow["remark_uid"]))
        {
            return mapRow["content"];
        }
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return "";
}

int Processor::addRemark(const long lUid, const FriendsProto::AddRemarkReq &req)
{
    return ReplaceRemark(lUid, req.remark_uid(), req.content(), 1);
}

int Processor::deleteRemark(const long lUid, const FriendsProto::DeleteRemarkReq &req)
{
    return ReplaceRemark(lUid, req.remark_uid(), "", 0);
}

int Processor::queryRemark(const long lUid, const FriendsProto::QueryRemarkReq &req, vector<FriendsProto::UserInfo> &vecUserInfo)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    string table_name = "tb_remark";
    std::vector<string> col_name = { "remark_uid", "content"};
    std::vector<vector<string>> whlist = {
        {"uid", "0", L2S(lUid)},
        {"state", "0", "1"}
    };
    if(!req.filter_content().empty())
    {
        vector<string> filter_content = {"content", "6", "%" + req.filter_content() + "%"};
        whlist.push_back(filter_content);
    }
    dbagent::TDBReadRsp dataRsp;
    iRet = readDataFromDBEx(lUid, table_name, col_name, whlist, "log_time", dataRsp);
    if(iRet != 0)
    {
        LOG_ERROR<<"select tb_game_brief err!"<< endl;
        return iRet;
    }

    for (auto it = dataRsp.records.begin(); it != dataRsp.records.end(); ++it)
    {
        map<string, string> mapRow;
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            mapRow.insert(std::make_pair(itfield->colName, itfield->colValue));
        }
        FriendsProto::UserInfo userinfo;
        userinfo.set_uid(S2L(mapRow["remark_uid"]));
        userinfo.set_remark_content(mapRow["content"]);

        userinfo::GetUserBasicReq basicReq;
        basicReq.uid = userinfo.uid();
        userinfo::GetUserBasicResp basicResp;
        iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(basicReq.uid)->getUserBasic(basicReq, basicResp);
        if (iRet != 0)
        {
            ROLLLOG_ERROR << "getUserBasic failed, uid: " << basicReq.uid << endl;
            continue;
        }
        userinfo.set_name(basicResp.userinfo.name);
        userinfo.set_head(basicResp.userinfo.head);
        userinfo.set_gender(basicResp.userinfo.gender);
        userinfo.set_signature(basicResp.userinfo.signature);
        vecUserInfo.push_back(userinfo);
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

string Processor::getRemarkContent(const long lUid, const long lRemarkID)
{
    FUNC_ENTRY("");
    __TRY__

    string table_name = "tb_remark";
    std::vector<string> col_name = { "remark_uid", "content"};
    std::vector<vector<string>> whlist = {
        {"uid", "0", L2S(lUid)},
        {"remark_uid", "0", L2S(lRemarkID)},
        {"state", "0", "1"}
    };

    dbagent::TDBReadRsp dataRsp;
    int iRet = readDataFromDBEx(lUid, table_name, col_name, whlist, "log_time", dataRsp);
    if(iRet != 0)
    {
        LOG_ERROR<<"select tb_game_brief err!"<< endl;
        return "";
    }

    for (auto it = dataRsp.records.begin(); it != dataRsp.records.end(); ++it)
    {
        map<string, string> mapRow;
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            mapRow.insert(std::make_pair(itfield->colName, itfield->colValue));
        }
        return mapRow["content"];
    }

    __CATCH__
    FUNC_EXIT("", 0);
    return "";
}

int Processor::insertSystemActivity(const global::CreateSystemActivityReq &req)
{
    int iRet = 0;
    FUNC_ENTRY("");

    Pb::SystemActivityInfo info;
    info.set_itype(req.iType);
    info.set_stitle(req.sTitle);
    info.set_siconlink(req.sIconLink);
    info.set_sdescript(req.sDescript);
    info.set_scontent(req.sContent);
    info.set_smorelink(req.sMoreLink);
    info.set_lfinishtime(req.lFinishTime);

    string s_info;
    info.SerializeToString(&s_info);

    long lTime = TNOW;
    string table_name = "tb_sys_activity_msg";
    std::map<string, string> col_info = {{"a_type", I2S(req.iType)},{"msg", s_info}, {"create_time", L2S(lTime)},{"finish_time", L2S(req.lFinishTime)}};

    iRet = writeDataFromDB(dbagent::E_INSERT, req.iType, table_name, col_info, {});
    if(iRet != 0)
    {
        LOG_ERROR << "write into db err. iRet: "<< iRet << endl;
        return iRet;
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::updateSystemActivity(const global::UpdateSystemActivityReq &req)
{
    int iRet = 0;
    FUNC_ENTRY("");

    Pb::SystemActivityInfo info;
    info.set_itype(req.iType);
    info.set_stitle(req.sTitle);
    info.set_siconlink(req.sIconLink);
    info.set_sdescript(req.sDescript);
    info.set_scontent(req.sContent);
    info.set_smorelink(req.sMoreLink);
    info.set_lfinishtime(req.lFinishTime);

    string s_info;
    info.SerializeToString(&s_info);

    string table_name = "tb_sys_activity_msg";
    std::map<string, string> whList = {{"id", L2S(req.lId)}};
    std::map<string, string> updateData = {{"a_type", I2S(req.iType)},{"msg", s_info}, {"finish_time", L2S(req.lFinishTime)},
    };
    iRet = writeDataFromDB(dbagent::E_UPDATE, req.iType, table_name, updateData, whList);
    if(iRet != 0)
    {
        LOG_ERROR << "write into db err. iRet: "<< iRet << endl;
        return iRet;
    }

    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::querySystemActivity(const long lUid, vector<string> &vecInfo)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    long lTime = TNOW;
    string table_name = "tb_sys_activity_msg";
    std::vector<string> col_name = { "id", "a_type", "create_time", "msg"};
    std::vector<vector<string>> whlist = {
        {"finish_time", "2", L2S(lTime)}
    };
   
    dbagent::TDBReadRsp dataRsp;
    iRet = readDataFromDBEx(lUid, table_name, col_name, whlist, "create_time", dataRsp);
    if(iRet != 0)
    {
        LOG_ERROR<<"select tb_sys_activity_msg err!"<< endl;
        return iRet;
    }

    for (auto it = dataRsp.records.begin(); it != dataRsp.records.end(); ++it)
    {
        map<string, string> mapRow;
        for (auto itfield = it->begin(); itfield != it->end(); ++itfield)
        {
            mapRow.insert(std::make_pair(itfield->colName, itfield->colValue));
        }

        Pb::SystemActivityInfo info;
        info.ParseFromString(mapRow["msg"]);
        info.set_lid(S2L(mapRow["id"]));
        info.set_lcreatetime(S2L(mapRow["create_time"]));
        info.set_scontent("");

        string sInfo;
        info.SerializeToString(&sInfo);
        vecInfo.push_back(sInfo);
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int Processor::insertServerUpdate(const long beginTime, const long endTime)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    int keyIndex = 10000;

    dataproxy::TWriteDataReq dataReq;
    dataReq.resetDefautlt();
    dataReq.keyName = I2S(E_REDIS_TYPE_HASH) + ":" + I2S(SERVER_UPDATE) + ":" + L2S(keyIndex);
    dataReq.operateType = E_REDIS_WRITE;
    dataReq.clusterInfo.resetDefautlt();
    dataReq.clusterInfo.busiType = E_REDIS_PROPERTY;
    dataReq.clusterInfo.frageFactorType = E_FRAGE_FACTOR_USER_ID;
    dataReq.clusterInfo.frageFactor = keyIndex;

    vector<TField> fields;
    TField tfield;
    tfield.colArithType = E_NONE;
    tfield.colName = "begin_time";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(beginTime);
    fields.push_back(tfield);
    tfield.colName = "end_time";
    tfield.colType = BIGINT;
    tfield.colValue = L2S(endTime);
    fields.push_back(tfield);
    dataReq.fields = fields;

    dataproxy::TWriteDataRsp dataRsp;
    iRet = g_app.getOuterFactoryPtr()->getDBAgentServantPrx(keyIndex)->redisWrite(dataReq, dataRsp);
    if (iRet != 0 || dataRsp.iResult != 0)
    {
        ROLLLOG_ERROR << "insert server update err, iRet: " << iRet << ", iResult: " << dataRsp.iResult << endl;
        return -2;
    }

    ROLLLOG_DEBUG << "insert server update, iRet: " << iRet << ", dataRsp: " << printTars(dataRsp) << endl;

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}
