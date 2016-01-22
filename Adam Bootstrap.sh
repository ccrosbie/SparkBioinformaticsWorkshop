mkdir maven
cd maven
echo "copying the maven zip file from the apache mirror"
wget http://apachemirror.ovidiudan.com/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
echo "unzipping the file"
tar -zxvf apache-maven-3.3.9-bin.tar.gz
echo "exporting MAVEN_HOME"
export PATH=$HOME/maven/apache-maven-3.3.9/bin:$PATH
cd $HOME
sudo yum install git
git clone https://github.com/bigdatagenomics/adam.git
cd adam
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m"
mvn clean package -DskipTests

