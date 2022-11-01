-- 调用方法获取当前时间
function get_corn_now()
{
    getNow();
}

-- 添加定时任务
function get_cron_time()
{
    println(concat("第一个任务：", getNow()));
}

-- 添加定时任务
function get_cron_time_1()
{
    println("测试添加任务！");
}