#include "GlobalServantImp.h"
#include "servant/Application.h"
#include "ServiceDefine.h"
#include "globe.h"
#include "LogComm.h"
#include "Push.h"
#include "JFGameHttpProto.h"
#include "CommonStruct.h"
#include "CommonCode.h"
#include "XGameComm.pb.h"
#include "GlobalServer.h"
#include "Processor.h"


#define PAGE_COUNT 10
//////////////////////////////////////////////////////

GlobalServantImp::GlobalServantImp()
{

}

GlobalServantImp::~GlobalServantImp()
{

}

void GlobalServantImp::initialize()
{

}


void GlobalServantImp::initNoticeMsg()
{
    //删除消息
    ProcessorSingleton::getInstance()->delDataFromDB(0, "tb_message", {{"message_type", "1"},});
    //插入系统消息
    vector<vector<string>> vecNotices;
    g_app.getOuterFactoryPtr()->getConfigServantPrx()->getSysNoticeCfg(vecNotices);

    LOG_DEBUG <<"initNoticeMsg size: "<< vecNotices.size() << endl;
    for(auto msg : vecNotices)
    {
        LOG_DEBUG <<"msg size: "<< msg.size() << endl;
        if(msg.size() != 2)
        {
            continue;
        }
        global::MessageReq req;
        req.iMailType = global::E_MESSAGE_TYPE(1);
        req.sTitle = msg[0];
        req.sContent = msg[1];
        ProcessorSingleton::getInstance()->insertMessage(req, true);
    }
}


//////////////////////////////////////////////////////
void GlobalServantImp::destroy()
{
    //destroy servant here:
    //...
}

void GlobalServantImp::initializeTimer()
{

}

//http请求处理接口
tars::Int32 GlobalServantImp::doRequest(const vector<tars::Char> &reqBuf, const map<std::string, std::string> &extraInfo, vector<tars::Char> &rspBuf, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    ROLLLOG_DEBUG << "recive request, reqBuf size : " << reqBuf.size() << endl;

    if (reqBuf.empty())
    {
        iRet = -1;
        return iRet;
    }

    THttpPackage thttpPack;
    if (!reqBuf.empty())
    {
        toObj(reqBuf, thttpPack);
    }

    if (thttpPack.vecData.empty())
    {
        iRet = -2;
        return iRet;
    }

    THttpPackage thttpPackRsp;
    thttpPackRsp.stUid.lUid = thttpPack.stUid.lUid;
    thttpPackRsp.stUid.sToken = thttpPack.stUid.sToken;
    thttpPackRsp.iVer = thttpPack.iVer;
    thttpPackRsp.iSeq = thttpPack.iSeq;
    tobuffer(thttpPackRsp, rspBuf);
    ROLLLOG_DEBUG << "response buff size: " << rspBuf.size() << endl;

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//tcp请求处理接口
tars::Int32 GlobalServantImp::onRequest(tars::Int64 lUin, const std::string &sMsgPack, const std::string &sCurServrantAddr, const JFGame::TClientParam &stClientParam, const JFGame::UserBaseInfoExt &stUserBaseInfo, tars::TarsCurrentPtr current)
{
    int iRet = 0;

    __TRY__

    ROLLLOG_DEBUG << "recv msg, uid : " << lUin << ", addr : " << stClientParam.sAddr << endl;

    async_response_onRequest(current, 0);

    XGameComm::TPackage pkg;
    pbToObj(sMsgPack, pkg);

    if (pkg.vecmsghead_size() == 0)
    {
        ROLLLOG_DEBUG << "package empty." << endl;
        return -1;
    }

    ROLLLOG_DEBUG << "recv msg, uid : " << lUin << ", msg : " << logPb(pkg) << endl;

    for (int i = 0; i < pkg.vecmsghead_size(); ++i)
    {
        int64_t ms1 = TNOWMS;

        const auto &head = pkg.vecmsghead(i);
        switch(head.nmsgid())
        {
        case XGameProto::ActionName::HALL_MESSAGE_QUERY:
        {
            GlobalProto::TQueryMessageReq queryMessageReq;
            pbToObj(pkg.vecmsgdata(i), queryMessageReq);
            iRet = onQueryMessages(pkg, queryMessageReq, sCurServrantAddr);
            break;
        }
        case XGameProto::ActionName::HALL_MESSAGE_DEAL:
        {
            GlobalProto::TDealMessageReq dealMessageReq;
            pbToObj(pkg.vecmsgdata(i), dealMessageReq);
            iRet = onDealMessages(pkg, dealMessageReq, sCurServrantAddr);
            break;
        }
        case XGameProto::ActionName::REDDOT_CHECK:
        {
            GlobalProto::RedDotCheckReq redDotCheckReq;
            pbToObj(pkg.vecmsgdata(i), redDotCheckReq);
            iRet = onRedDotCheck(pkg, redDotCheckReq, sCurServrantAddr);
            break;
        }
        case XGameProto::ActionName::REDDOT_SET:
        {
            GlobalProto::RedDotSetReq redDotSetReq;
            pbToObj(pkg.vecmsgdata(i), redDotSetReq);
            iRet = onRedDotSet(pkg, redDotSetReq, sCurServrantAddr);
            break;
        }
        case XGameProto::ActionName::FRIENDS_ADD:
        {
            FriendsProto::AddFriendReq addFriendReq;
            pbToObj(pkg.vecmsgdata(i), addFriendReq);
            iRet = onAddFriend(pkg, addFriendReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::FRIENDS_DELETE:
        {
            FriendsProto::DeleteFriendReq deleteFriendReq;
            pbToObj(pkg.vecmsgdata(i), deleteFriendReq);
            iRet = onDeleteFriend(pkg, deleteFriendReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::FRIENDS_QUERY:
        {
            FriendsProto::QueryFriendListReq queryFriendListReq;
            pbToObj(pkg.vecmsgdata(i), queryFriendListReq);
            iRet = onQueryFriends(pkg, queryFriendListReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::FRIENDS_QUERYAPPLICANT:
        {
            FriendsProto::QueryApplicantListReq queryApplicantListReq;
            pbToObj(pkg.vecmsgdata(i), queryApplicantListReq);
            iRet = onQueryFriendApplicants(pkg, queryApplicantListReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::FRIENDS_AGREETOADD:
        {
            FriendsProto::AgreeToAddReq agreeToAddReq;
            pbToObj(pkg.vecmsgdata(i), agreeToAddReq);
            iRet = onAgreeToAdd(pkg, agreeToAddReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::FRIENDS_DETAIL:
        {
            FriendsProto::QueryFriendDetailReq queryFriendDetailReq;
            pbToObj(pkg.vecmsgdata(i), queryFriendDetailReq);
            iRet = onFriendDetail(pkg, queryFriendDetailReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::REMARK_ADD:
        {
            FriendsProto::AddRemarkReq addRemarkReq;
            pbToObj(pkg.vecmsgdata(i), addRemarkReq);
            iRet = onAddRemark(pkg, addRemarkReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::REMARK_DELETE:
        {
            FriendsProto::DeleteRemarkReq deleteRemarkReq;
            pbToObj(pkg.vecmsgdata(i), deleteRemarkReq);
            iRet = onDeleteRemark(pkg, deleteRemarkReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::REMARK_QUERY:
        {
            FriendsProto::QueryRemarkReq queryRemarkReq;
            pbToObj(pkg.vecmsgdata(i), queryRemarkReq);
            iRet = onQueryRemark(pkg, queryRemarkReq, sCurServrantAddr, current);
            break;
        }
        case XGameProto::ActionName::SYSTEM_ACTIVITY_LIST:
        {
            GlobalProto::SystemActivityInfoReq queryReq;
            pbToObj(pkg.vecmsgdata(i), queryReq);
            iRet = onQuerySystemActivity(pkg, queryReq, sCurServrantAddr, current);
            break;
        }
        default:
        {
            ROLLLOG_ERROR << "invalid msg id, uid: " << lUin << ", msg id: " << head.nmsgid() << endl;
            break;
        }
        }

        if (iRet != 0)
        {
            ROLLLOG_ERROR << "Process msg fail, uid: " << lUin << ", msg id: " << head.nmsgid()  << ", iRet: " << iRet << endl;
        }
        else
        {
            ROLLLOG_DEBUG << "Process msg succ, uid: " << lUin << ", msg id: " << head.nmsgid() << endl;
        }

        int64_t ms2 = TNOWMS;
        if ((ms2 - ms1) > COST_MS)
        {
            ROLLLOG_WARN << "@Performance, msgid:" << head.nmsgid() << ", costTime:" << (ms2 - ms1) << endl;
        }
    }

    __CATCH__
    return iRet;
}

tars::Int32 GlobalServantImp::genMessage(const global::MessageReq &req, global::MessageResp &resp, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    iRet = ProcessorSingleton::getInstance()->insertMessage(req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "insert message err. "<< endl;
    }

    resp.iResultCode = iRet;

    __CATCH__

    FUNC_EXIT("", iRet);

    return iRet;
}

tars::Int32 GlobalServantImp::serverUpdateMessage(long beginTime, long endTime, const string &title, const string &content, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    global::MessageReq req;
    req.iMailType = global::E_MESSAGE_TYPE(1);
    req.sTitle = title;
    req.sContent = content;
    iRet = ProcessorSingleton::getInstance()->insertMessage(req, true);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "insert message err. "<< endl;
        return iRet;
    }

    iRet = ProcessorSingleton::getInstance()->insertServerUpdate(beginTime, endTime);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "insert server update info err. "<< endl;
        return iRet;
    }

    __CATCH__

    FUNC_EXIT("", iRet);

    return iRet;
}

tars::Int32 GlobalServantImp::updateMessage(const global::UpdateMessageReq &req, global::UpdateMessageResp &resp, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    map<string, string> whlist = {{"message_index", req.sMessageIndex}, {"message_type", I2S(int(req.iMailType))}};
    iRet = ProcessorSingleton::getInstance()->updateMessages(req.lPlayerID, {{"message_state", L2S(req.iState)},}, whlist);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query message err. uid: "<< req.lPlayerID << endl;
    }

    resp.iResultCode = iRet;

    __CATCH__

    FUNC_EXIT("", iRet);

    return iRet;
}

tars::Int32 GlobalServantImp::createSystemActivity(const global::CreateSystemActivityReq &req, global::CreateSystemActivityResp &resp, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    iRet = ProcessorSingleton::getInstance()->insertSystemActivity(req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query message err. type: "<< req.iType << endl;
    }

    resp.iResultCode = iRet;

    __CATCH__

    FUNC_EXIT("", iRet);

    return iRet;
}

tars::Int32 GlobalServantImp::updateSystemActivity(const global::UpdateSystemActivityReq &req, global::UpdateSystemActivityResp &resp, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    iRet = ProcessorSingleton::getInstance()->updateSystemActivity(req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query message err. id: "<< req.lId << endl;
    }

    resp.iResultCode = iRet;

    __CATCH__

    FUNC_EXIT("", iRet);

    return iRet;
}


tars::Int32 GlobalServantImp::checkFriend(tars::Int64 lUin, tars::Int64 lFriendID, tars::TarsCurrentPtr current)
{
    return ProcessorSingleton::getInstance()->checkFriendRelation(lUin, lFriendID, FriendsProto::E_FRIEND) ? 1 : 0;
}

std::string GlobalServantImp::getRemark(tars::Int64 lUin, tars::Int64 lRemarkID, tars::TarsCurrentPtr current)
{
    return ProcessorSingleton::getInstance()->getRemarkContent(lUin, lRemarkID);
}

//查询系统消息
tars::Int32 GlobalServantImp::onQueryMessages(const XGameComm::TPackage &pkg, const GlobalProto::TQueryMessageReq &req, const std::string &sCurServrantAddr)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    GlobalProto::TQueryMessageResp resp;
    iRet = ProcessorSingleton::getInstance()->queryMessages(pkg.stuid().luid(), req, resp);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query message err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::HALL_MESSAGE_QUERY, resp);

    __CATCH__

    FUNC_EXIT("", iRet);
    return iRet;
}

//处理系统消息
tars::Int32 GlobalServantImp::onDealMessages(const XGameComm::TPackage &pkg, const GlobalProto::TDealMessageReq &req, const std::string &sCurServrantAddr)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    GlobalProto::TDealMessageResp resp;
    //处理消息
    iRet = ProcessorSingleton::getInstance()->dealMessages(req.lmessageid(), req.istate());
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "deal message err. uid: "<< pkg.stuid().luid() << endl;
    }
    else
    {
        iRet = ProcessorSingleton::getInstance()->updateMessages(pkg.stuid().luid(), {{"message_state", L2S(req.istate())},}, {{"message_id", L2S(req.lmessageid())},});
        if(iRet != 0)
        {
            ROLLLOG_ERROR << "query message err. uid: "<< pkg.stuid().luid() << endl;
        }
    }

    resp.set_resultcode(iRet);
    resp.set_lmessageid(req.lmessageid());
    resp.set_istate(req.istate());
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::HALL_MESSAGE_DEAL, resp);

    __CATCH__

    FUNC_EXIT("", iRet);
    return iRet;
}

//获取红点
tars::Int32 GlobalServantImp::onRedDotCheck(const XGameComm::TPackage &pkg, const GlobalProto::RedDotCheckReq &req, const std::string &sCurServrantAddr)
{
    FUNC_ENTRY("");

    int iRet = 0;

    __TRY__

    long uid = pkg.stuid().luid();
    vector<global::RedDotInfo> infos;

    GlobalProto::RedDotCheckResp resp;
    int iRed = 0;

    //处理消息
    iRet = ProcessorSingleton::getInstance()->queryRedDot(uid, req.iflag(), infos);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query reddot err. uid: "<< pkg.stuid().luid() << endl;
    }
    else
    {
        int iLimitCount = 7;
        if (req.iflag() == 0)
        {
            bool bIsFind = false;
            int iFlag = 0;
            // 补充info
            for (int i = 1; i <= iLimitCount; ++i)
            {
                iFlag = i == iLimitCount ? 20 : i;
                bIsFind = false;
                for (auto item : infos)
                {
                    if (item.iFlag == iFlag)
                    {
                        bIsFind = true;
                        break;
                    }
                }

                if (!bIsFind)
                {
                    global::RedDotInfo info;
                    info.iFlag = iFlag;
                    info.lExtend = 0;
                    info.iRead = TNOW - 7 * 24 * 60 * 60; // 7天前
                    infos.push_back(info);
                }
            }

            int count = 0;
            for (auto item : infos)
            {
                count = 0;
                if (item.iFlag == 6)
                {
                    if (iRed == 1)
                    {
                        continue;
                    }
                    iRet = ProcessorSingleton::getInstance()->queryActvitysForRedDot(uid, item.iRead, count);
                }
                else if (item.iFlag == 20)
                {
                    iRet = ProcessorSingleton::getInstance()->queryFriendApplyForRedDot(uid, item.iRead, count);
                }
                else if (item.iFlag == 30)
                {
                    // 联盟先不管
                }
                else
                {
                    if (iRed == 1)
                    {
                        continue;
                    }
                    iRet = ProcessorSingleton::getInstance()->queryMessagesForRedDot(uid, item.iFlag, item.iRead, count);
                }
                if (iRet == 0 && count > 0)
                {
                    if (item.iFlag != 20)
                    {
                        iRed |= 1;
                    }
                    else
                    {
                        iRed |= 2;
                    }
                }
            }
        }
        else if (req.iflag() == 1)
        {
            bool bIsFind = false;
            int iFlag = 0;
            // 补充info
            for (int i = 1; i <= iLimitCount; ++i)
            {
                iFlag = i == iLimitCount ? 30 : i;
                bIsFind = false;
                for (auto item : infos)
                {
                    if (item.iFlag == iFlag)
                    {
                        bIsFind = true;
                        break;
                    }
                }

                if (!bIsFind)
                {
                    global::RedDotInfo info;
                    info.iFlag = iFlag;
                    info.lExtend = 0;
                    info.iRead = TNOW - 7 * 24 * 60 * 60; // 7天前
                    infos.push_back(info);
                }
            }

            int count = 0;
            for (auto item : infos)
            {
                count = 0;
                if (item.iFlag == 6)
                {
                    iRet = ProcessorSingleton::getInstance()->queryActvitysForRedDot(uid, item.iRead, count);
                    if (iRet == 0 && count > 0)
                    {
                        GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                        pInfo->set_itype(item.iFlag);
                        pInfo->set_lextend(0);
                        pInfo->set_icount(count);
                    }
                }
                else if (item.iFlag == 30)
                {
                    int unionCount = 0;
                    std::string sNid = "(";
                    vector<long> vUnionIds;
                    iRet = ProcessorSingleton::getInstance()->queryUnionId(uid, vUnionIds);
                    if (iRet == 0)
                    {
                        for (auto nId : vUnionIds)
                        {
                            if (unionCount > 0)
                            {
                                sNid += ", ";
                            }
                            unionCount++;

                            sNid += L2S(nId);
                        }

                        sNid += ")";
                    }

                    if (unionCount > 0)
                    {
                        int count = 0;
                        iRet = ProcessorSingleton::getInstance()->queryUnionApplyForRedDot(uid, sNid, item.iRead, count);
                        if (iRet == 0 && count > 0)
                        {
                            GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                            pInfo->set_itype(30);
                            pInfo->set_lextend(0);
                            pInfo->set_icount(count);
                        }
                    }
                }
                else
                {
                    iRet = ProcessorSingleton::getInstance()->queryMessagesForRedDot(uid, item.iFlag, item.iRead, count);
                    if (iRet == 0 && count > 0)
                    {
                        GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                        pInfo->set_itype(item.iFlag);
                        pInfo->set_lextend(0);
                        pInfo->set_icount(count);
                    }
                }
            }
        }
        else if (req.iflag() == 2)
        {
            // 获取俱乐部ID
            vector<long> vClubIds;
            iRet = ProcessorSingleton::getInstance()->queryClubId(uid, vClubIds);
            if (iRet == 0)
            {
                bool bIsFind = false;
                // 补充info
                for (auto cId : vClubIds)
                {
                    bIsFind = false;
                    for (auto item : infos)
                    {
                        if (item.lExtend == cId)
                        {
                            bIsFind = true;
                            break;
                        }
                    }

                    if (!bIsFind)
                    {
                        global::RedDotInfo info;
                        info.iFlag = 10;
                        info.lExtend = cId;
                        info.iRead = TNOW - 7 * 24 * 60 * 60; // 7天前
                        infos.push_back(info);
                    }
                }

                int count = 0;
                for (auto item : infos)
                {
                    count = 0;
                    iRet = ProcessorSingleton::getInstance()->queryClubApplyForRedDot(uid, item.lExtend, item.iRead, count);
                    if (iRet == 0 && count > 0)
                    {
                        GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                        pInfo->set_itype(item.iFlag);
                        pInfo->set_lextend(item.lExtend);
                        pInfo->set_icount(count);
                    }
                }
            }
        }
        else if (req.iflag() == 4)
        {
            int iRead = TNOW - 7 * 24 * 60 * 60; // 7天前
            for (auto item : infos)
            {
                if (item.iFlag == 20)
                {
                    iRead = item.iRead;
                    break;
                }
            }

            int count = 0;
            iRet = ProcessorSingleton::getInstance()->queryFriendApplyForRedDot(uid, iRead, count);
            if (iRet == 0 && count > 0)
            {
                GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                pInfo->set_itype(20);
                pInfo->set_lextend(0);
                pInfo->set_icount(count);
            }
        }
        else if (req.iflag() == 5)
        {
            int unionCount = 0;
            std::string sNid = "(";
            vector<long> vUnionIds;
            iRet = ProcessorSingleton::getInstance()->queryUnionId(uid, vUnionIds);
            if (iRet == 0)
            {
                for (auto nId : vUnionIds)
                {
                    if (unionCount > 0)
                    {
                        sNid += ", ";
                    }
                    unionCount++;

                    sNid += L2S(nId);
                }

                sNid += ")";
            }

            if (unionCount > 0)
            {
                int iRead = TNOW - 7 * 24 * 60 * 60; // 7天前
                int count = 0;
                iRet = ProcessorSingleton::getInstance()->queryUnionApplyForRedDot(uid, sNid, iRead, count);
                if (iRet == 0 && count > 0)
                {
                    GlobalProto::RedDotInfo *pInfo = resp.add_infos();
                    pInfo->set_itype(30);
                    pInfo->set_lextend(0);
                    pInfo->set_icount(count);
                }
            }
        }
    }


    resp.set_resultcode(iRet);
    resp.set_iflag(req.iflag());
    resp.set_ired(iRed);

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::REDDOT_CHECK, resp);

    __CATCH__

    FUNC_EXIT("", iRet);
    return iRet;
}

//红点设置阅读
tars::Int32 GlobalServantImp::onRedDotSet(const XGameComm::TPackage &pkg, const GlobalProto::RedDotSetReq &req, const std::string &sCurServrantAddr)
{
    FUNC_ENTRY("");

    int iRet = 0;
    __TRY__

    long uid = pkg.stuid().luid();

    GlobalProto::RedDotSetResp resp;
    //处理消息
    iRet = ProcessorSingleton::getInstance()->replaceRedDot(uid, req.itype(), req.lextend());
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "set red dot err. uid: "<< uid << endl;
    }


    resp.set_resultcode(0);
    resp.set_itype(req.itype());
    resp.set_lextend(req.lextend());

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::REDDOT_SET, resp);

    __CATCH__

    FUNC_EXIT("", iRet);
    return iRet;
}


//
int GlobalServantImp::onAddFriend(const XGameComm::TPackage &pkg, const FriendsProto::AddFriendReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");

    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    FriendsProto::AddFriendResp resp;
    iRet = ProcessorSingleton::getInstance()->addFriend(pkg.stuid().luid(), req, resp);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "add friend err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_ADD, resp);

    // 红点通知
    if (iRet == 0)
    {
        std::vector<long> uIdList;
        uIdList.push_back(req.friend_uid());
        PushMessage(req.friend_uid(), (int)push::E_PUSH_MSG_TYPE_FRIEND_NOTIFY, uIdList);
    }
    __CATCH__

    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onDeleteFriend(const XGameComm::TPackage &pkg, const FriendsProto::DeleteFriendReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    FriendsProto::DeleteFriendResp resp;
    iRet = ProcessorSingleton::getInstance()->deleteFriend(pkg.stuid().luid(), req, resp);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "delete friend err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_DELETE, resp);

    if(iRet == 0 )
    {
        pushAddOrDelFriend(req.friend_uid(), pkg.stuid().luid(), 0);
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onQueryFriends(const XGameComm::TPackage &pkg, const FriendsProto::QueryFriendListReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    vector<FriendsProto::UserInfo> vecUserInfo;
    iRet = ProcessorSingleton::getInstance()->queryFriends(pkg.stuid().luid(), FriendsProto::E_FRIEND, vecUserInfo);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query friend err. uid: "<< pkg.stuid().luid() << endl;
    }

    int startIndex = (int (req.icurrpage() -1)) * PAGE_COUNT;
    startIndex = startIndex < 0 ? 1 : startIndex;
    int endIndex = startIndex + PAGE_COUNT;
    endIndex = endIndex > int(vecUserInfo.size()) -1 ? int(vecUserInfo.size()) -1 : endIndex;

    LOG_DEBUG << "startIndex: "<< startIndex<< ", endIndex: "<< endIndex<< ", size :"<< vecUserInfo.size() << endl;

    FriendsProto::QueryFriendListResp resp;
    for(int i = startIndex; i <= endIndex; i++)
    {
        auto ptr = resp.add_friendlist();
        ptr->CopyFrom(vecUserInfo[i]);
    }

    resp.set_resultcode(iRet);
    resp.set_icurrpage(req.icurrpage());
    resp.set_ipagecount(vecUserInfo.size() / PAGE_COUNT  + 1);

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_QUERY, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onQueryFriendApplicants(const XGameComm::TPackage &pkg, const FriendsProto::QueryApplicantListReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    vector<FriendsProto::UserInfo> vecUserInfo;
    iRet = ProcessorSingleton::getInstance()->queryFriends(pkg.stuid().luid(), FriendsProto::E_BE_APPLIED, vecUserInfo);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query friend err. uid: "<< pkg.stuid().luid() << endl;
    }

    int startIndex = (int (req.icurrpage() -1)) * PAGE_COUNT;
    int endIndex = startIndex + PAGE_COUNT;
    endIndex = endIndex > int(vecUserInfo.size()) -1 ? int(vecUserInfo.size()) -1 : endIndex;

    FriendsProto::QueryApplicantListResp resp;
    for(int i = startIndex; i <= endIndex; i++)
    {
        auto ptr = resp.add_applicantlist();
        ptr->CopyFrom(vecUserInfo[i]);
    }

    resp.set_resultcode(iRet);
    resp.set_icurrpage(req.icurrpage());
    resp.set_ipagecount(vecUserInfo.size() / PAGE_COUNT  + 1);

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_QUERYAPPLICANT, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

void GlobalServantImp::pushAddOrDelFriend(const long lUid, const long other_uid, const int iType)
{
    userinfo::GetUserBasicReq basicReq;
    basicReq.uid = other_uid;
    userinfo::GetUserBasicResp basicResp;
    int iRet = g_app.getOuterFactoryPtr()->getHallServantPrx(basicReq.uid)->getUserBasic(basicReq, basicResp);
    if (iRet != 0)
    {
        ROLLLOG_ERROR << "getUserBasic failed, uid: " << basicReq.uid << endl;
        return;
    }

    //推送添加好友消息
    push::PushMsgReq pushMsgReq;
    push::AddFriendNotify addFriendNotify;
    addFriendNotify.uid = other_uid;
    addFriendNotify.name = basicResp.userinfo.name;
    addFriendNotify.head = basicResp.userinfo.head;
    addFriendNotify.gender = basicResp.userinfo.gender;
    addFriendNotify.logout_time = basicResp.userinfo.lastLoginTime > basicResp.userinfo.lastLogoutTime ? 0 : TNOW - basicResp.userinfo.lastLogoutTime;
    addFriendNotify.type = iType;

    push::PushMsg pushMsg;
    pushMsg.uid = lUid;
    pushMsg.msgType = push::E_PUSH_MSG_TYPE_ADD_FRIEND;
    tobuffer(addFriendNotify, pushMsg.vecData);
    pushMsgReq.msg.push_back(pushMsg);
    g_app.getOuterFactoryPtr()->getPushServantPrx(pushMsg.uid)->async_pushMsg(NULL, pushMsgReq);
}

//
int GlobalServantImp::onAgreeToAdd(const XGameComm::TPackage &pkg, const FriendsProto::AgreeToAddReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    FriendsProto::AgreeToAddResp resp;
    iRet = ProcessorSingleton::getInstance()->agreeToAdd(pkg.stuid().luid(), req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "delete friend err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    resp.set_is_agree(req.is_agree());
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_AGREETOADD, resp);

    if(iRet == 0)
    {
        pushAddOrDelFriend(req.friend_uid(), pkg.stuid().luid(), 1);
    }

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int GlobalServantImp::onFriendDetail(const XGameComm::TPackage &pkg, const FriendsProto::QueryFriendDetailReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
     FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    FriendsProto::QueryFriendDetailResp resp;
    iRet = ProcessorSingleton::getInstance()->friendDetail(pkg.stuid().luid(), req.uid(), resp);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << " friend detail err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::FRIENDS_DETAIL, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onAddRemark(const XGameComm::TPackage &pkg, const FriendsProto::AddRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    LOG_DEBUG << "req: "<< logPb(req)<< endl;

    FriendsProto::AddRemarkResp resp;
    iRet = ProcessorSingleton::getInstance()->addRemark(pkg.stuid().luid(), req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "add remark err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    resp.set_content(req.content());
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::REMARK_ADD, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onDeleteRemark(const XGameComm::TPackage &pkg, const FriendsProto::DeleteRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    FriendsProto::DeleteRemarkResp resp;
    iRet = ProcessorSingleton::getInstance()->deleteRemark(pkg.stuid().luid(), req);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "add remark err. uid: "<< pkg.stuid().luid() << endl;
    }

    resp.set_resultcode(iRet);
    resp.set_remark_uid(req.remark_uid());
    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::REMARK_DELETE, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//
int GlobalServantImp::onQueryRemark(const XGameComm::TPackage &pkg, const FriendsProto::QueryRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    vector<FriendsProto::UserInfo> vecUserInfo;
    iRet = ProcessorSingleton::getInstance()->queryRemark(pkg.stuid().luid(), req, vecUserInfo);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query remark err. uid: "<< pkg.stuid().luid() << endl;
    }

    int startIndex = (int (req.icurrpage() -1)) * PAGE_COUNT;
    int endIndex = startIndex + PAGE_COUNT;
    endIndex = endIndex > int(vecUserInfo.size()) -1 ? int(vecUserInfo.size()) -1 : endIndex;

    FriendsProto::QueryRemarkResp resp;
    for(int i = startIndex; i <= endIndex; i++)
    {
        auto ptr = resp.add_remarklist();
        ptr->CopyFrom(vecUserInfo[i]);
    }

    resp.set_resultcode(iRet);
    resp.set_icurrpage(req.icurrpage());
    resp.set_ipagecount(vecUserInfo.size() / PAGE_COUNT  + 1);

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::REMARK_QUERY, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

int GlobalServantImp::onQuerySystemActivity(const XGameComm::TPackage &pkg, const GlobalProto::SystemActivityInfoReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current)
{
    FUNC_ENTRY("");
    int iRet = 0;
    __TRY__

    vector<string> vecInfo;
    iRet = ProcessorSingleton::getInstance()->querySystemActivity(pkg.stuid().luid(), vecInfo);
    if(iRet != 0)
    {
        ROLLLOG_ERROR << "query remark err. uid: "<< pkg.stuid().luid() << endl;
    }

    int startIndex = (int (req.icurrpage() -1)) * PAGE_COUNT;
    int endIndex = startIndex + PAGE_COUNT;
    endIndex = endIndex > int(vecInfo.size()) -1 ? int(vecInfo.size()) -1 : endIndex;

    GlobalProto::SystemActivityInfoResp resp;
    for(int i = startIndex; i <= endIndex; i++)
    {
        resp.add_infos(vecInfo[i]);
    }

    resp.set_resultcode(iRet);
    resp.set_icurrpage(req.icurrpage());
    resp.set_ipagecount(vecInfo.size() / PAGE_COUNT  + 1);

    ROLLLOG_DEBUG << "resp: "<< logPb(resp) << endl;
    toClientPb(pkg, sCurServrantAddr, XGameProto::ActionName::SYSTEM_ACTIVITY_LIST, resp);

    __CATCH__
    FUNC_EXIT("", iRet);
    return iRet;
}

//发送消息到客户端
template<typename T>
int GlobalServantImp::toClientPb(const XGameComm::TPackage &tPackage, const std::string &sCurServrantAddr, XGameProto::ActionName actionName, const T &t)
{
    XGameComm::TPackage rsp;
    rsp.set_iversion(tPackage.iversion());

    auto ptuid = rsp.mutable_stuid();
    ptuid->set_luid(tPackage.stuid().luid());
    rsp.set_igameid(tPackage.igameid());
    rsp.set_sroomid(tPackage.sroomid());
    rsp.set_iroomserverid(tPackage.iroomserverid());
    rsp.set_isequence(tPackage.isequence());
    rsp.set_iflag(tPackage.iflag());

    auto mh = rsp.add_vecmsghead();
    mh->set_nmsgid(actionName);
    mh->set_nmsgtype(XGameComm::MSGTYPE::MSGTYPE_RESPONSE);
    mh->set_servicetype(XGameComm::SERVICE_TYPE::SERVICE_TYPE_GLOBAL);
    rsp.add_vecmsgdata(pbToString(t));

    auto pPushPrx = Application::getCommunicator()->stringToProxy<JFGame::PushPrx>(sCurServrantAddr);
    if (pPushPrx)
    {
        ROLLLOG_DEBUG << "toclient pb: " << logPb(rsp) << ", t: " << logPb(t) << endl;
        pPushPrx->tars_hash(tPackage.stuid().luid())->async_doPushBuf(NULL, tPackage.stuid().luid(), pbToString(rsp));
    }
    else
    {
        ROLLLOG_ERROR << "pPushPrx is null: " << logPb(rsp) << ", t: " << logPb(t) << endl;
    }

    return 0;
}

tars::Int32 GlobalServantImp::doCustomMessage(bool bExpectIdle)
{
    return 0;
}

void GlobalServantImp::PushMessage(const long uId, const int iType, std::vector<long> uIdList)
{
    push::PushMsgReq pushMsgReq;
    push::PushMsg pushMsg;
    pushMsg.msgType = (push::E_Push_Msg_Type)iType;
    for (auto uId : uIdList)
    {
        pushMsg.uid = uId;
        pushMsgReq.msg.push_back(pushMsg);
    }
    g_app.getOuterFactoryPtr()->getPushServantPrx(uId)->async_pushMsg(NULL, pushMsgReq);
}
