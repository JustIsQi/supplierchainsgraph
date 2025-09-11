# 查询"宁德时代新能源科技股份有限公司" 相连的所有边
GO FROM "宁德时代新能源科技股份有限公司" OVER * YIELD type(edge) AS relation_type, dst(edge) AS target_vertex, properties(edge) AS relation_properties;

# 查询"宁德时代新能源科技股份有限公司" 相连的指定边信息
GO FROM "宁德时代新能源科技股份有限公司" OVER PRODUCES YIELD type(edge) AS relation_type, dst(edge) AS target_vertex, properties(edge) AS relation_properties;

#  查询"宁德时代新能源科技股份有限公司" 相连的指定边完整信息
```
GO FROM "宁德时代新能源科技股份有限公司" OVER CUSTOMER_OF REVERSELY 
YIELD 
    edge AS e,
    src(edge) AS source_vertex, 
    dst(edge) AS destination_vertex,
    properties($$) AS destination_vertex_properties,  
    properties($^) AS source_vertex_properties,       
    properties(edge) AS edge_properties;              
```
# 匹配两个点的特定关系，注意点的方向
MATCH (a)-[e:CUSTOMER_OF]->(b) WHERE id(b) == "威马" AND id(a) == "宁德时代新能源科技股份有限公司" RETURN e


# 数据入库计划
-  20250819：优先处理半年报告、年度报告,其中篇幅最长、信息最多，但更新频率不高；其他临时公告等更新频率高，但篇幅短、信息少
