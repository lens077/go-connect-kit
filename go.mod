module github.com/lens077/go-connect-kit

// 这里的版本号是**天花板**，不是「用最新的」。
//
// 消费方目前有两个：ecommerce（go 1.27.0）与 control-tower（go 1.26.5）。
// 本模块的 go directive 必须小于等于所有消费方里最低的那一个，否则低版本
// 那一侧直接编译不过，而且报错发生在**别人的仓库里**，很难定位。
//
// 想抬高它之前，先确认所有消费方都已经升上去。
go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/lib/pq v1.12.3
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/text v0.29.0 // indirect
)
