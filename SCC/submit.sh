echo "Do you want to package first? (N/y)"
read answer
if [ -z "$answer" ] || [ "$answer" == "${answer#[Yy]}" ] ;then
    echo "Skip packaging"
else
    sudo /usr/local/sbt/sbt package
fi
# 删除output目录
if [ -d "output" ]; then
    echo "output directory exists, deleting it"
    sudo rm -r output
fi
echo "Press enter to continue"
read
/usr/local/spark/bin/spark-submit --class "SparkSCC" ./target/scala-2.12/simple-project_2.12-1.0.jar