#ifndef _GlobalServantImp_H_
#define _GlobalServantImp_H_

#include "servant/Application.h"
#include "GlobalServant.h"
#include "XGameComm.pb.h"
#include "CommonCode.pb.h"
#include "CommonStruct.pb.h"
#include "config.pb.h"
#include "globe.h"
#include "LogComm.h"
#include "JFGameHttpProto.h"
#include "OuterFactoryImp.h"
#include "GlobalProto.h"
#include "global.pb.h"
#include "RedDot.pb.h"
#include "Friends.pb.h"

//
using namespace std;
using namespace JFGame;
using namespace JFGameHttpProto;
using namespace global;


/**
 *
 *服务接口
 */
class GlobalServantImp : public GlobalServant
{
public:
    /**
     *
     */
    GlobalServantImp();

    /**
     *
     */
    virtual ~GlobalServantImp();

    /**
     *
     */
    virtual void initialize();

    /**
     *
     */
    virtual void destroy();

    /**
     *
     */
    void initializeTimer();

public:
    //HTTP请求处理接口
    virtual tars::Int32 doRequest(const vector<tars::Char> &reqBuf, const map<std::string, std::string> &extraInfo, vector<tars::Char> &rspBuf, tars::TarsCurrentPtr current);
    //TCP请求处理接口
    virtual tars::Int32 onRequest(tars::Int64 lUin, const std::string &sMsgPack, const std::string &sCurServrantAddr, const JFGame::TClientParam &stClientParam, const JFGame::UserBaseInfoExt &stUserBaseInfo, tars::TarsCurrentPtr current);

    //产生消息
    virtual tars::Int32 genMessage(const global::MessageReq &req, global::MessageResp &resp, tars::TarsCurrentPtr current);

    //停服消息
    virtual tars::Int32 serverUpdateMessage(long beginTime, long endTime, const string &title, const string &content, tars::TarsCurrentPtr current);

    //产生消息
    virtual tars::Int32 updateMessage(const global::UpdateMessageReq &req, global::UpdateMessageResp &resp, tars::TarsCurrentPtr current);

    //创建活动
    virtual tars::Int32 createSystemActivity(const global::CreateSystemActivityReq &req, global::CreateSystemActivityResp &resp, tars::TarsCurrentPtr current);

    //更新活动
    virtual tars::Int32 updateSystemActivity(const global::UpdateSystemActivityReq &req, global::UpdateSystemActivityResp &resp, tars::TarsCurrentPtr current);

    //检查好友
    virtual tars::Int32 checkFriend(tars::Int64 lUin, tars::Int64 lFriendID, tars::TarsCurrentPtr current);

    //获取备注
    virtual std::string getRemark(tars::Int64 lUin, tars::Int64 lRemarkID, tars::TarsCurrentPtr current);

public:

    void initNoticeMsg();

    void pushAddOrDelFriend(const long lUid, const long other_uid, const int iType);

    //查询消息
    tars::Int32 onQueryMessages(const XGameComm::TPackage &pkg, const GlobalProto::TQueryMessageReq &req, const std::string &sCurServrantAddr);
    //处理消息
    tars::Int32 onDealMessages(const XGameComm::TPackage &pkg, const GlobalProto::TDealMessageReq &req, const std::string &sCurServrantAddr);
    //获取红点
    tars::Int32 onRedDotCheck(const XGameComm::TPackage &pkg, const GlobalProto::RedDotCheckReq &req, const std::string &sCurServrantAddr);
    //设置红点阅读
    tars::Int32 onRedDotSet(const XGameComm::TPackage &pkg, const GlobalProto::RedDotSetReq &req, const std::string &sCurServrantAddr);
    //
    int onAddFriend(const XGameComm::TPackage &pkg, const FriendsProto::AddFriendReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);
    //
    int onDeleteFriend(const XGameComm::TPackage &pkg, const FriendsProto::DeleteFriendReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onQueryFriends(const XGameComm::TPackage &pkg, const FriendsProto::QueryFriendListReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onQueryFriendApplicants(const XGameComm::TPackage &pkg, const FriendsProto::QueryApplicantListReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onAgreeToAdd(const XGameComm::TPackage &pkg, const FriendsProto::AgreeToAddReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onFriendDetail(const XGameComm::TPackage &pkg, const FriendsProto::QueryFriendDetailReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onAddRemark(const XGameComm::TPackage &pkg, const FriendsProto::AddRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onDeleteRemark(const XGameComm::TPackage &pkg, const FriendsProto::DeleteRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onQueryRemark(const XGameComm::TPackage &pkg, const FriendsProto::QueryRemarkReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

    int onQuerySystemActivity(const XGameComm::TPackage &pkg, const GlobalProto::SystemActivityInfoReq &req, const std::string &sCurServrantAddr, tars::TarsCurrentPtr current);

public:
    //时钟周期回调
    virtual tars::Int32 doCustomMessage(bool bExpectIdle = false);

private:
    //发送消息到客户端
    template<typename T>
    int toClientPb(const XGameComm::TPackage &tPackage, const std::string &sCurServrantAddr, XGameProto::ActionName actionName, const T &t);

private:
    // 推送
    void PushMessage(const long uId, const int iType, std::vector<long> uIdList);
};

/////////////////////////////////////////////////////
#endif
