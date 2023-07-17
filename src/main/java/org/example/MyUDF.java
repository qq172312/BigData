package org.example;

import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletImpl;

/**
 * @Create 2023/5/16 17:55
 */
public class MyUDF extends GenericUDF {

    /*
    *
    *   初始化工作,计算之前。
    */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length!=1){
            throw new UDFArgumentException("只接受一个参数");
        }

        ObjectInspector argument = arguments[0];
        if(ObjectInspector.Category.PRIMITIVE!=argument.getCategory()){
            throw new UDFArgumentException("只接受基本数据类型参数");
        }

        PrimitiveObjectInspector argument1 = (PrimitiveObjectInspector) argument;
        if(PrimitiveObjectInspector.PrimitiveCategory.STRING!=argument1.getPrimitiveCategory()){
            throw new UDFArgumentException("不是字符串类型");
        }
        //使用工厂方法返回int值(根据需求)
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /*
        每处理一行数据就会调用一次下面方法
    */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object o = argument.get();

        if(o==null){
            return 0;
        }else {
            //获取字符串长度
            return o.toString().length();
        }

    }

    //获取执行计划中解释的字符串
    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
