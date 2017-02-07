package com.dataartisans;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class ClasspathTest {
    public static void main(String[] args) throws Exception {
        ClassLoader parentClassloader = new URLClassLoader(new URL[]{new File("/home/robert/.m2/repository/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar").toURI().toURL()}, ClasspathTest.class.getClassLoader());
        parentClassloader.loadClass("com.google.protobuf.TextFormat");
        ClassLoader childClassloader = new URLClassLoader(new URL[]{new File("/home/robert/flink-workdir/quickstart-1.2-tests/target/quickstart-1.2-tests-1.0-SNAPSHOT.jar").toURI().toURL()}, parentClassloader);
        Class<?> clazz = childClassloader.loadClass("com.google.protobuf.TextFormatterTest");
        Object instance = clazz.newInstance();
        Method m = instance.getClass().getMethod("escapeIt", String.class);
        String escaped = (String) m.invoke(instance, "test ' me ");
        System.out.println("Escaped = " + escaped);
    }
}
