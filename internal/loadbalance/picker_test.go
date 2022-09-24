package loadbalance_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/kumatch-sandbox/proglog/internal/loadbalance"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}

	for _, method := range []string{
		"/log.vX.Log/Produce",
		"/log.vX.Log/Consume",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}
		result, err := picker.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

func TestPickerProducesToLeader(t *testing.T) {
	picker, subConns := setupPickerTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX/Produce",
	}

	for i := 0; i < 5; i++ {
		got, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], got.SubConn)
	}
}

func TestPickerConsumesToLeader(t *testing.T) {
	picker, subConns := setupPickerTest()
	info := balancer.PickInfo{
		FullMethodName: "/log.vX/Consume",
	}

	for i := 0; i < 5; i++ {
		got, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[i%2+1], got.SubConn)
	}
}

func setupPickerTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn

	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}

	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}

		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}

	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)

	return picker, subConns
}

// subConn は、balancer.SubConn を実装している
type subConn struct {
	addrs []resolver.Address
}

func (sc *subConn) UpdateAddresses(addrs []resolver.Address) {
	sc.addrs = addrs
}

func (sc *subConn) Connect() {}
