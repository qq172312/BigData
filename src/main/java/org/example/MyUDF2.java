package org.example;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @Create 2023/5/22 14:28
 */
public class MyUDF2 extends GenericUDF {

    /*
        进行初始化校验工作
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        //判断参数个数
        if(arguments.length!=1){
            throw new UDFArgumentException("只接受一个参数");
        }

        //判断是否为基本数据类型
        ObjectInspector argument = arguments[0];
        if(argument.getCategory()!= ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("只接受基本数据类型");
        }

        //判断是否为字符串
        PrimitiveObjectInspector argument1 = (PrimitiveObjectInspector) argument;
        if(argument1.getPrimitiveCategory()!= PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentException("只接受字符串类型");
        }

        //通过工厂方法返回int值
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;

    }

    /*
        处理数据
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object o = argument.get();
        if(o==null){
            return 0;
        }else {
            return o.toString().substring(0,1).toUpperCase()+o.toString().substring(1,o.toString().length()-1);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
