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

-- 测试定义 dic
function my_dic(my_keys:list, my_dic:dic)
{
   --let user_dic = {"emp_no": "000C", "level": 20, "emp": {"name": "吴大富", "age": 100}};
   --user_dic.get("emp").get(my_key);
   match {
      my_dic.contains?(my_keys.first()): match {
                                           my_keys.count() > 1: my_dic(my_keys.rest(), my_dic.get(my_keys.first()));
                                           my_keys.count() == 1: my_dic.get(my_keys.first());
                                        }
   }
}

function my_dic(my_key:string)
{
   let dic = {"emp_no": "000C", "level": 20, "emp": {"name": "吴大富", "age": 100}};
   dic.get("emp").get(my_key);
}
my_dic('name');


function get_dic_vs(ps_keys:list)
{
    innerFunction {
       function my_dic(ps_keys:list, ps_dics:dic)
       {
          --let user_dic = {"emp_no": "000C", "level": 20, "emp": {"name": "吴大富", "age": 100}};
          --user_dic.get("emp").get(my_key);
          match {
             ps_dics.contains?(ps_keys.first()): match {
                                                  ps_keys.count() > 1: my_dic(ps_keys.rest(), ps_dics.get(ps_keys.first()));
                                                  ps_keys.count() == 1: ps_dics.get(ps_keys.first());
                                               }
          }
       }
    }

    let user_dic = {"emp_no": "000C", "level": 20, "emp": {"name": "吴大富", "age": 100}};
    my_dic(ps_keys, user_dic);
}

get_dic_vs(["emp", "name"]);

get_dic_vs(["emp"]);
get_dic_vs(["level"]);

function show_list()
{
    let a = [1, 2+3*4, "yes", true];
    a;
}
show_list();

-- 测度定义 list