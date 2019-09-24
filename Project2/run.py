import sys
import os
import shutil

if __name__ == '__main__':
    project_name = sys.argv[1]
    input_file = sys.argv[2]
    input_string = ""
    if sys.argv.__len__() == 4:
        input_string = sys.argv[3]
        print(input_string)
    input_path = "fileInput/%s/" % input_file
    output_path = "fileOutput/%s/" % project_name
    class_path = "class/%s_classes" % project_name
    java_file_path = "src/%s.java" % project_name
    jar_file_path = "jar/%s.jar" % project_name
    cmd2 = "javac -classpath C:/Java/jdk/lib/hadoop-core-1.2.1.jar -d %s %s" % (class_path, java_file_path)
    cmd3 = "jar -cvf %s -C %s/ ." % (jar_file_path, class_path)
    cmd4 = "hadoop jar %s %s %s %s" % (jar_file_path, project_name, input_path, output_path)
    if input_string != "":
        cmd4 = cmd4 + " " + input_string
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    if os.path.exists(class_path):
        pass
    else:
        os.makedirs(class_path)
    for cmd in [cmd2, cmd3, cmd4]:
        print(cmd)
        os.system(cmd)
