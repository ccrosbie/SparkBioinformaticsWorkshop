/* This script installs maven, Git, clones ADAM repository from GitHub and uses Maven to build ADAM */

/* create a new directory to install maven */
mkdir maven

cd maven

/* download the maven zip from the apache mirror website */

echo "copying the maven zip file from the apache mirror"


wget http://apachemirror.ovidiudan.com/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz

echo "unzipping the file"

tar -zxvf apache-maven-3.3.9-bin.tar.gz

/* export the “MAVEN_HOME” path */

echo "exporting MAVEN_HOME"

export PATH=$HOME/maven/apache-maven-3.3.9/bin:$PATH


/* install git in the home directory */

cd $HOME

sudo yum install git


/* clone the ADAM repository from GitHub and build the package */

git clone https://github.com/bigdatagenomics/adam.git

cd adam


export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m"

mvn clean package -DskipTests

