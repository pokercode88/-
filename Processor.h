#ifndef _Processor_H_
#define _Processor_H_

//
#include <util/tc_singleton.h>
#include "DataProxyProto.h"
#include "global.pb.h"
#include "RedDot.pb.h"
#include "GlobalProto.h"
#include "UserInfoProto.h"
#include "Friends.pb.h"

//
using namespace tars;
using namespace dbagent;
/**
 *请求处理类
 *
 */
class Processor
{
public:
    /**
     *
    */
    Processor();

    /**
     *
    */
    ~Processor();

public:
    int readDataFromDBEx(long uid, const string& table_name, const std::vector<string>& col_name, const std::vector<vector<string>>& whlist, const string& order_col, dbagent::TDBReadRsp &dataRsp);

    int readDataFromDB(long uid, const string& table_name, const std::vector<string>& col_name, const map<string, string>& whlist, const string& order_col, int limit_num,  dbagent::TDBReadRsp &dataRsp);

    int writeDataFromDB(dbagent::Eum_Query_Type dBOPType, long uid, const string& table_name, const std::map<string, string>& col_info, const map<string, string>& whlist);

    int delDataFromDB(long uid, const string& table_name, const map<string, string>& whlist);

public:

    int queryMessages(const long lUid, const GlobalProto::TQueryMessageReq &req, GlobalProto::TQueryMessageResp& resp);

    int dealMessages(const long messageId, const int iStatus);

    int updateMessages(const long lUid, const map<string, string>& updateData, const map<string, string>& whlData);

    int insertMessage(const global::MessageReq &req, bool bSysNotice = false);

    int replaceRedDot(const long lUid, const int iFlag, const long lExtend);

    int queryRedDot(const long lUid, const int iFlag, vector<global::RedDotInfo> &infos);

    int queryMessagesForRedDot(const long lUid, const int iType, const int iRead, int &count);

    int queryClubId(const long lUid, vector<long> &vClubIds);

    int queryClubApplyForRedDot(const long lUid, const long lCid, const int iRead, int &count);

    int queryFriendApplyForRedDot(const long lUid, const int iRead, int &count);

    int queryUnionId(const long lUid, vector<long> &vUnionIds);

    int queryUnionApplyForRedDot(const long lUid, const std::string sNid, const int iRead, int &count);

    int queryActvitysForRedDot(const long lUid, const int iRead, int &count);

    bool checkFriendRelation(tars::Int64 uid, tars::Int64 friend_uid, FriendsProto::Eum_Friend_Relationship_Type relationship);

    int InsertFriendEntry(tars::Int64 uid, tars::Int64 friend_uid, std::string content, FriendsProto::Eum_Friend_Relationship_Type relationship);

    int UpdateFriendEntry(tars::Int64 uid, tars::Int64 friend_uid, FriendsProto::Eum_Friend_Relationship_Type relationship);

    int addFriend(const long lUid, const FriendsProto::AddFriendReq &req, FriendsProto::AddFriendResp &resp);

    int deleteFriend(const long lUid, const FriendsProto::DeleteFriendReq &req, FriendsProto::DeleteFriendResp &resp);

    int queryFriends(const long lUid, FriendsProto::Eum_Friend_Relationship_Type relationship, vector<FriendsProto::UserInfo> &vecUserInfo);

    int agreeToAdd(const long lUid, const FriendsProto::AgreeToAddReq &req);

    int friendDetail(const long lUid, const long friend_uid, FriendsProto::QueryFriendDetailResp &resp);

    int ReplaceRemark(const long lUid, tars::Int64 remark_uid, std::string content, int state);

    string getUserRemark(const long lUid, const long remark_uid);

    int addRemark(const long lUid, const FriendsProto::AddRemarkReq &req);

    int deleteRemark(const long lUid, const FriendsProto::DeleteRemarkReq &req);

    int queryRemark(const long lUid, const FriendsProto::QueryRemarkReq &req, vector<FriendsProto::UserInfo> &vecUserInfo);

    string getRemarkContent(const long lUid, const long lRemarkID);

    int insertSystemActivity(const global::CreateSystemActivityReq &req);

    int updateSystemActivity(const global::UpdateSystemActivityReq &req);

    int querySystemActivity(const long lUid, vector<string> &vecInfo);

    int insertServerUpdate(const long beginTime, const long endTime);
};

//singleton
typedef TC_Singleton<Processor, CreateStatic, DefaultLifetime> ProcessorSingleton;

#endif

