package rtm2_sdk

import base "github.com/tomasliu-agora/rtm2-base/v2"

func generateHeader(uri int32, m Marshalable) *Header {
	buffer, _ := m.Marshal()
	return &Header{Uri: uri, Message: buffer}
}

func getUriFromReq(req interface{}) int32 {
	switch req.(type) {
	case *base.LoginReq:
		return UriLogin
	case *base.LogoutReq:
		return UriLogout
	case *base.MessageSubReq:
		return UriMessageSubscribe
	case *base.MessageUnsubReq:
		return UriMessageUnsubscribe
	case *base.MessagePublishReq:
		return UriMessagePublish
	case *base.StreamJoinReq:
		return UriStreamJoin
	case *base.StreamLeaveReq:
		return UriStreamLeave
	case *base.StreamJoinTopicReq:
		return UriStreamJoinTopic
	case *base.StreamMessageReq:
		return UriStreamPublish
	case *base.StreamLeaveTopicReq:
		return UriStreamLeaveTopic
	case *base.StreamSubTopicReq:
		return UriStreamSubTopic
	case *base.StreamUnsubTopicReq:
		return UriStreamUnsubTopic
	case *base.StreamSubListReq:
		return 0 // TODO
	case *base.StorageChannelReq:
		return UriStorageOpChannelMetaData
	case *base.StorageChannelGetReq:
		return UriStorageGetChannelMetaData
	case *base.StorageUserReq:
		return UriStorageOpUserMetaData
	case *base.StorageUserGetReq:
		return UriStorageGetUserMetaData
	case *base.StorageUserSubReq:
		return UriStorageSubscribeUserMetaData
	case *base.StorageUserUnsubReq:
		return UriStorageUnSubscribeUserMetaData
	case *base.PresenceWhereNowReq:
		return UriPresenceWhereNow
	case *base.PresenceWhoNowReq:
		return UriPresenceWhoNow
	case *base.PresenceSetStateReq:
		return UriPresenceSetState
	case *base.PresenceGetStateReq:
		return UriPresenceGetState
	case *base.PresenceRemoveStateReq:
		return UriPresenceRemoveState
	case *base.SetParamsReq:
		return UriSetParam
	case *base.RenewTokenReq:
		return UriRenewToken
	case *base.LockSetReq:
		return UriLockSet
	case *base.LockRemoveReq:
		return UriLockRemove
	case *base.LockGetReq:
		return UriLockGet
	case *base.LockAcquireReq:
		return UriLockAcquire
	case *base.LockReleaseReq:
		return UriLockRelease
	case *base.LockRevokeReq:
		return UriLockRevoke
	case base.TokenPrivilegeExpire:
		return UriTokenPrivilegeExpire
	default:
		return 0
	}
}

func unmarshalResp(uri int32, errCode int32, message []byte) (interface{}, int32, error) {
	switch uri {
	case UriStreamSubTopic:
		resp := &base.StreamSubTopicResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriStorageGetChannelMetaData:
		resp := &base.StorageChannelGetResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriStorageGetUserMetaData:
		resp := &base.StorageUserGetResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriPresenceWhoNow:
		resp := &base.PresenceWhoNowResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriPresenceWhereNow:
		resp := &base.PresenceWhereNowResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriPresenceGetState:
		resp := &base.PresenceGetStateResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	case UriLockGet:
		resp := &base.LockGetResp{}
		err := resp.Unmarshal(message)
		if err != nil {
			return nil, errCode, err
		} else {
			return resp, errCode, nil
		}
	}
	return nil, 0, nil
}
