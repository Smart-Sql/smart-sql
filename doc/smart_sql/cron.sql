-- 调用方法获取当前时间
function get_corn_now()
{
    get_now();
}

-- 添加定时任务
function get_cron_time()
{
    println(concat("第一个任务：", get_now()));
}

-- 添加定时任务
function get_cron_time_1()
{
    println("测试添加任务！");
}

-- 定时任务添加数据到
function save_db()
{
   query_sql("INSERT INTO public.Categories (CategoryName, Description, Picture) VALUES('Seafood',?, '')", [get_now()]);
}