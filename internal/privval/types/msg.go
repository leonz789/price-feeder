// package types contains message types for oracle stream communication with remote signer.
package types

import "errors"

func MustWrapMsg(msg any) (re OracleStreamMessage) {
	switch m := msg.(type) {
	case *SignPriceFeedRequest:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_SignPriceFeedRequest{
				SignPriceFeedRequest: m,
			},
		}
	case *SignPriceFeedResponse:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_SignPriceFeedResponse{
				SignPriceFeedResponse: m,
			},
		}
	case *Ping:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_Ping{
				Ping: m,
			},
		}
	case *Pong:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_Pong{
				Pong: m,
			},
		}
	case *GetPubKeyRequest:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_GetPubKeyRequest{
				GetPubKeyRequest: m,
			},
		}
	case *GetPubKeyResponse:
		re = OracleStreamMessage{
			Sum: &OracleStreamMessage_GetPubKeyResponse{
				GetPubKeyResponse: m,
			},
		}
	default:
		// log unknown message type
	}
	return
}

func (sr *OracleStreamMessage_SignPriceFeedRequest) SetID(id uint64) {
	sr.SignPriceFeedRequest.Id = id
}

func (sg *OracleStreamMessage_GetPubKeyRequest) SetID(id uint64) {
	sg.GetPubKeyRequest.Id = id
}

func (om *OracleStreamMessage) GetBytesResponse() (uint64, []byte, error) {
	switch x := om.Sum.(type) {
	case *OracleStreamMessage_SignPriceFeedResponse:
		return x.SignPriceFeedResponse.RequestId, x.SignPriceFeedResponse.Signature, nil
	case *OracleStreamMessage_GetPubKeyResponse:
		return x.GetPubKeyResponse.RequestId, x.GetPubKeyResponse.PublicKey, nil
	default:
		return 0, nil, errors.New("unsupported message type for GetBytesResponse")
	}
}

func (om *OracleStreamMessage) SetID(id uint64) bool {
	switch x := om.Sum.(type) {
	case *OracleStreamMessage_SignPriceFeedRequest:
		x.SignPriceFeedRequest.Id = id
		return true
	case *OracleStreamMessage_GetPubKeyRequest:
		x.GetPubKeyRequest.Id = id
	}
	return false
}
