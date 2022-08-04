-- 测试调用方法
function my_line(a:string, b:string)
{
    concat(a, "-", b);
}

-- 测试调用方法
function get_range_id(id:int, my_count:varchar)
{
    let rs = [];
    for (i in range(my_count))
    {
        rs.add(id + i);
    }
    rs;
}