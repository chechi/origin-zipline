from zipline.api import symbol,order,record,set_benchmark


def initialize(context):
    context.asset = symbol('600600')
    set_benchmark(symbol('600600'))


def handle_data(context, data):
    # print('handle_data')
    # print(data.current([symbol('603520'), symbol('600600')],
    #                    ['open', 'close', 'high', 'low', 'volume', 'price', 'last_traded']))
    # 返回订单ID
    order(symbol('600600'), 100)
    # record(STL=data.current(context.asset, 'price'))

    # 可以查看账号信息
    # print(context.account)


def analyze(context, analyze_data):
    pass
    # print(analyze_data)
    # print(analyze_data.STL)