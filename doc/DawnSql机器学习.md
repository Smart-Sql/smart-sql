# Dawn Sql 机器学习

## 1、DawnSql 中机器学习的流程
1. 生成训练数据集 (Double 数据类型的矩阵)
2. 在训练数据集上，训练模型
3. 输入要预测的向量，预测结果

**在 Dawn Sql 中机器学习模型是支持分布式计算的！可以持续地进行学习，可以在最新数据到来之时实时地对决策进行改进！**

**Dawn Sql 机器学习的优势：**
1. 数据无需移动。增加对业务的响应时间的同时，还减少了开发的投入和设备
>其它平台：模型是在不同的系统中训练和部署（训练结束之后）的，数据科学家需要等待ETL或者其它的数据传输过程，来将数据移至比如Apache Mahout或者Apache Spark这样的系统进行训练，然后还要等待这个过程结束并且将模型部署到生产环境。在系统间移动TB级的数据可能花费数小时的时间，此外，训练部分通常发生在旧的数据集上。
>Dawn Sql 中是不需要移动数据的，模型直接就可以在里面训练和部署，而且可以持续的进行学习，实时的对决策进行改进。

2. 使用分布式解决，机器学习和深度学习需要处理的数据量不断增长的问题，使用分布式计算来加速模型的训练。
>现状：机器学习和深度学习需要处理的数据量不断增长，已经无法放在单一的服务器上。这促使数据科学家要么提出更复杂的解决方案，要么切换到比如Spark或者TensorFlow这样的分布式计算平台上。但是这些平台通常只能解决模型训练的一部分问题，这给开发者之后的生产部署带来了很多的困难。
>Dawn Sql 使用分布式计算，可以持续地进行学习，可以在最新数据到来之时实时地对决策进行改进！

3. 用 Dawn Sql 来调用训练，预测模型，简单高效。

## 2、生成训练数据集 (Double 数据类型的矩阵)
```sql
-- 定一个分布式的矩阵
create_train_matrix({"table_name": "训练数据集"});
-- 如果这个分布式的矩阵在 public 下面
-- is_clustering
create_train_matrix({"table_name": "训练数据集", "schema_name": "public", "is_clustering": true});

-- 是否有个叫 "训练数据集" 的矩阵
has_train_matrix({"table_name": "训练数据集"});
-- 如果这个分布式的矩阵在 public 下面
has_train_matrix({"table_name": "训练数据集", "schema_name": "public"});

-- 删除有个叫 "训练数据集" 的矩阵
drop_train_matrix({"table_name": "训练数据集"});
-- 如果这个分布式的矩阵在 public 下面
drop_train_matrix({"table_name": "训练数据集", "schema_name": "public"});

-- 为分布式矩阵添加数据
train_matrix({"table_name": "训练数据集", "value": [1, 2, 3], "label": 123});
-- 如果这个分布式的矩阵在 public 下面
train_matrix({"table_name": "训练数据集", "schema_name": "public", "value": [1, 2, 3], "label": 123});

-- 如果要添加的是聚类的就不要 lable 例如：
-- 为分布式矩阵添加数据 
train_matrix({"table_name": "训练数据集", "value": [1, 2, 3]});
-- 如果这个分布式的矩阵在 public 下面
train_matrix({"table_name": "训练数据集", "schema_name": "public", "value": [1, 2, 3]});

-- 生成训练数据集
function create_train_data()
{
   let rs = query_sql("sql");
   for (r in rs)
   {
       train_matrix({"table_name": "训练数据集", "value": [r.nth(1), r.nth(2), r.nth(3)], "label": r.nth(4)});
   }
}

-- 或者使用内置函数 load_csv("csv 地址"); 这个只能在 Dbeaver 中使用
loadCsv({"table_name": "", "csv_path": "csv 地址"});
-- 如果这个分布式的矩阵在 public 下面
loadCsv({"table_name": "", "schema_name": "public", "csv_path": "csv 地址"});

-- 可以在 Dbeaver 中通过函数 show_train_data 查看数据，一般我们只需要看前面 100 多项即可，系统默认为 1000 项。因为是分布式数据，如果量很大的话，容易把内存撑爆。
show_train_data({"table_name": "", "item_size": 1000});
-- 如果这个分布式的矩阵在 public 下面
show_train_data({"table_name": "", "schema_name": "public", "item_size": 1000});
```

## 3、标准化和归一化
### 3.1、归一化：
最大最小值归一化 （MinMaxScaler）公式：$x_{new}=\frac{x-min(x)}{max(x)-min(x)}$

例如：v = [12, -1, 30, 5]; 
最小值：min(v) = -1;
最大值：max(v) = 30;
$v_{new} = [\frac{12+1}{30+1}, \frac{-1+1}{30+1}, \frac{30+1}{30+1}, \frac{5+1}{30+1}]$


### 3.2、标准化：
标准化 (StandardScaler) 公式：$x_{new}=\frac{x-\mu}{\sigma}$

$\mu$：是均值
$\sigma$：是标准差

### 3.3、应用场景
对需要计算距离的算法，会用到标准化和归一化，在 Dawn Sql 中，我们对于需要计算距离的算法都自动进行了标准化。**这样做的好处是即使是对算法细节不太熟悉的人都可以很容易的使用**
如果要设置成归一化：
```sql
-- 例如：归一化 
fit({"table_name": "house_prices", "ml_func_name": "LinearRegressionLSQR", "preprocessor": "MinMaxScaler"});

-- 例如：标准化。默认为标准化
fit({"table_name": "house_prices", "ml_func_name": "LinearRegression", "preprocessor": "StandardScaler"});

-- 等价于
fit({"table_name": "house_prices", "ml_func_name": "LinearRegression"});
```


## 4、线性回归

### 4.1、用线性回归训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：LinearRegressionLSQR 
表示一种稀疏线性方程和稀疏最小二乘的算法
参考：https://web.stanford.edu/group/SOL/software/lsqr/lsqr-toms82a.pdf

ml_func_name：LinearRegressionSGD 
表示随机梯度下降
LinearRegressionSGD 参数
maxIterations ：最大迭代次数 (默认 1000)
batchSize：批量大小 (默认 10)
locIterations：局部迭代次数 (默认 100)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用线性回归训练数据
fit({"table_name": "house_prices", "ml_func_name": "LinearRegressionLSQR"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "LinearRegressionSGD", "params": {"maxIterations": 1234, "batchSize": 15}});
```

### 4.2、用线程回归预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: 输入的参数

```sql
-- 用线性回归训练数据
predict({"table_name": "house_prices", "ml_func_name": "LinearRegression"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "LinearRegressionSGD"});
```

## 5、逻辑回归 (二元分类)
### 5.1、用逻辑回归训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：LogisticRegression 
LogisticRegression 参数
learningRate：学习率  (默认 0.2)
maxIterations ：最大迭代次数 (默认 1000)
batchSize：批量大小 (默认 10)
locIterations：局部迭代次数 (默认 100)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用线性回归训练数据
fit({"table_name": "house_prices", "ml_func_name": "LogisticRegression", "params": {"maxIterations": 1234, "batchSize": 15}});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "LogisticRegression", "params": {"maxIterations": 1234, "batchSize": 15}});
```

### 5.2、用逻辑回归预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用线性回归训练数据
predict({"table_name": "house_prices", "ml_func_name": "LogisticRegression"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "LogisticRegression"});
```

## 6、SVM 线性分类(二元分类)
### 6.1、用SVM训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：SVM 

```sql
-- 用SVM训练数据
fit({"table_name": "house_prices", "ml_func_name": "SVM"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "SVM"});
```

### 6.2、用SVM预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用SVM训练数据
predict({"table_name": "house_prices", "ml_func_name": "SVM"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "SVM"});
```

## 7、决策树分类(二元分类)
### 7.1、用决策树分类训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：DecisionTreeClassification 
DecisionTreeClassification 的参数
maxDeep：树分枝的最大深度 (默认 5)
minImpurityDecrease：节点分枝最小纯度增长量 (默认 0)

```sql
-- 用 DecisionTreeClassification 训练数据
fit({"table_name": "house_prices", "ml_func_name": "DecisionTreeClassification"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "DecisionTreeClassification", "params": {"maxDeep": 10, "minImpurityDecrease": 0}});
```

### 7.2、用决策树分类预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 DecisionTreeClassification 训练数据
predict({"table_name": "house_prices", "ml_func_name": "DecisionTreeClassification"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "DecisionTreeClassification"});
```

## 8、决策树回归
### 8.1、用决策树回归训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：DecisionTreeRegression 
DecisionTreeRegression 的参数
maxDeep：树分枝的最大深度 (默认 5)
minImpurityDecrease：节点分枝最小纯度增长量 (默认 0)

```sql
-- 用 DecisionTreeRegression 训练数据
fit({"table_name": "house_prices", "ml_func_name": "DecisionTreeRegression"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "DecisionTreeRegression", "params": {"maxDeep": 10, "minImpurityDecrease": 0}});
```

### 8.2、用决策树回归预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 DecisionTreeRegression 训练数据
predict({"table_name": "house_prices", "ml_func_name": "DecisionTreeRegression"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "DecisionTreeRegression"});
```

**分类树与回归树**
**分类决策树可用于处理离散型数据，回归决策树可用于处理连续型数据**

## 9、KNN 分类 (二元分类，label 为 0 或 1)
### 9.1、用KNN分类训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：KNNClassification 
KNNClassification 的参数
k：邻居的数量 (默认 5)

```sql
-- 用 KNNClassification 训练数据
fit({"table_name": "house_prices", "ml_func_name": "KNNClassification"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KNNClassification", "params": {"maxDeep": 10, "amountOfTrees": 100}});
```

### 9.2、用KNN分类预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 KNNClassification 训练数据
predict({"table_name": "house_prices", "ml_func_name": "KNNClassification"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KNNClassification"});
```

## 10、KNN 回归
### 10.1、用KNN回归训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：KNNRegression 
KNNClassification 的参数
k：邻居的数量 (默认 5)

```sql
-- 用 KNNRegression 训练数据
fit({"table_name": "house_prices", "ml_func_name": "KNNRegression"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KNNRegression", "params": {"maxDeep": 10, "amountOfTrees": 100}});
```

### 10.2、用KNN回归预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 KNNRegression 训练数据
predict({"table_name": "house_prices", "ml_func_name": "KNNRegression"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KNNRegression"});
```

## 11、随机森林分类(多分类)
### 11.1、用随机森林分类训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：RandomForestClassification 
RandomForestClassification 的参数
amountOfTrees：树的数量 (默认 5)
maxDepth：树的最大深度
minImpurityDelta：决策树生长的最小纯净度 (默认 0)
subSampleSize：子样本大小  (默认 1)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用 RandomForestClassification 训练数据
fit({"table_name": "house_prices", "ml_func_name": "RandomForestClassification"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "RandomForestClassification", "params": {"maxDeep": 10, "amountOfTrees": 100}});
```

### 11.2、用随机森林分类预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 RandomForestClassification 训练数据
predict({"table_name": "house_prices", "ml_func_name": "RandomForestClassification"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "RandomForestClassification"});
```

## 12、随机森林回归
### 12.1、用随机森林回归训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：RandomForestRegression 
RandomForestRegression 的参数
amountOfTrees：树的数量 (默认 5)
maxDepth：树的最大深度
minImpurityDelta：决策树生长的最小纯净度 (默认 0)
subSampleSize：子样本大小  (默认 1)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用 RandomForestRegression 训练数据
fit({"table_name": "house_prices", "ml_func_name": "RandomForestRegression"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "RandomForestRegression", "params": {"maxDeep": 10, "amountOfTrees": 100}});
```

### 12.2、用随机森林回归预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 RandomForestRegression 训练数据
predict({"table_name": "house_prices", "ml_func_name": "RandomForestRegression"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "RandomForestRegression"});
```

## 13、k-means 聚类(非监督学习)
### 13.1、用k-means 聚类训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：KMeans 
KMeans 的参数
k：聚类数量 (默认 2)
maxIterations：收敛前的最大迭代次数 (默认 10)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用 KMeans 训练数据
fit({"table_name": "house_prices", "ml_func_name": "KMeans"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KMeans", "params": {"k": 5, "maxIterations": 20}});
```

### 13.2、用k-means 聚类预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 KMeans 训练数据
predict({"table_name": "house_prices", "ml_func_name": "KMeans"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "KMeans"});
```

## 14、GMM 高斯混合模型聚类 (非监督学习)
### 14.1、用GMM 聚类训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：GMM 
GMM 的参数
numberOfComponents：高斯模型的个数，即聚类个数 (默认 2)
maxCountOfIterations：最大迭代次数 (默认 10)
maxCountOfClusters：最大聚类数量 (默认 10)
seed：随机生成器的种子 (默认 1234)

```sql
-- 用 KMeans 训练数据
fit({"table_name": "house_prices", "ml_func_name": "GMM"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "GMM", "params": {"k": 5, "maxIterations": 20}});
```

### 14.2、用GMM 聚类预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 GMM 训练数据
predict({"table_name": "house_prices", "ml_func_name": "GMM"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "GMM"});
```

## 15、神经网络
### 15.1、用神经网络训练数据
训练数据的函数：fit(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法
params: {}  机器学习的参数

ml_func_name：NeuralNetwork 
NeuralNetwork 的参数
maxIterations：最大迭代次数 (默认 100)
batchSize：批量大小（每个分区）(默认 100)
locIterations：最大局部迭代次数 (默认 100)
seed：随机生成器的种子 (默认 1234)

Layer：表示层
例如：Layer: [{"neuronsCnt": 10, "hasBias": false, "activationFunction": "RELU"}]
Layer 里面的参数：
neuronsCnt: 在新的层中神经元的数据量
hasBias：是否有偏置项
activationFunction：激活函数

```sql
-- 用 NeuralNetwork 训练数据
fit({"table_name": "house_prices", "ml_func_name": "NeuralNetwork"});
-- 如果分布式式矩阵在 public 下面
fit({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "NeuralNetwork", "params": {"Layer": [{"neuronsCnt": 10, "hasBias": false, "activationFunction": "RELU"}], "maxIterations": 50}});
```

### 15.2、用神经网络预测数据

训练数据的函数：predict(参数)。参数为一个hash table ：
table_name: 表名, 
schema_name: 要么是 public, 要么不设置, 
ml_func_name: 机器学习的方法

```sql
-- 用 GMM 训练数据
predict({"table_name": "house_prices", "ml_func_name": "NeuralNetwork"});
-- 如果分布式式矩阵在 public 下面
predict({"table_name": "house_prices", "schema_name": "public", "ml_func_name": "NeuralNetwork"});
```