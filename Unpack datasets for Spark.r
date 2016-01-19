#pre-Spark file building. This is needed because gz is not a splittable format.
#########]
###get the file listing from S3
system("mkdir -p temp")
padd_url = "s3://crosbie-tutorials/PRAD2/cgcc/"
s3_2_file <- function (bkname,x)
#INPUT: S3 folder/bucket
#OUTPUT: The name of the file created containing a list of files in S3. 
{
  textfile = paste(bkname, ".txt", sep="")
  if (x > 0)
    {
    textfile = paste("temp/", paste(x, ".txt", sep=""), sep="")
    padd_url = paste(padd_url,bkname,sep="")
  }
  cmd = (paste("aws s3 ls ", padd_url))
  cmd = paste(paste(cmd, " > "), textfile, sep="")
  system(cmd)
  return (textfile)
}

library("uuid")
library("stringi")

unpackgztar <- function(compresedfile,link)
#takes in the .tar.gz. files, pulls down a local copy, extracts, loads back into S3 and does clean up
{
  uncompressedfile <-  stri_replace_all(compresedfile,fixed=".tar.gz","")
  S3path <- paste(paste(padd_url, link,sep=""),compresedfile,sep="")
  S3cmdCP1 <- paste("aws s3 cp ",S3path, sep="")
  
  S3cmdCP <- paste(S3cmdCP1, compresedfile,sep=" ")
  system(S3cmdCP)
  
  S3cmdDe <- paste("tar -xvf ",compresedfile)
  system(S3cmdDe)
  
  "aws s3 cp uncompressedfile S3://link/uncompressed"
  S3cpBack <- paste("aws s3 sync ", uncompressedfile,sep="") 
  S3cpBack <- paste(S3cpBack,padd_url, sep=" ")
  S3cpBack <- paste(S3cpBack,link, sep="")
  S3cpBack <- paste(S3cpBack,uncompressedfile,sep="")
  system(S3cpBack)
  
  S3cmdRM <- paste("rm ",compresedfile)
  system(S3cmdRM)
  
  S3cmdRM <- paste("rm -r ",uncompressedfile)
  system(S3cmdRM)
  
}

rec_builder <- function(folders,path) {
#recursive function to loop through the folder/PRE(key-prefix) structure of S3
  for (i in folders) 
  {
    if (!is.na(i) & i != "PRE" & i != "")
    {
      #print("starting folder:")
      #print(i)
      
      lnk <- paste(path,i,sep="")
      #print("new path:")
      #print(lnk)

      x <- read.table(s3_2_file(lnk,UUIDgenerate()), header=F,fill=T)
      PRE <- x[x["V1"] == "PRE"]
      OBJ <- x[x["V1"] != "PRE"]
      for (p in PRE)
      {
         rec_builder(p,lnk)
      }
      for (o in OBJ)
      {
        if (stri_sub(o,-7,-1) == ".tar.gz")
        {
          unpackgztar(o,lnk) 
        }
      }
      
    }
  }
}

#get a list of the foldrers 
#main callng routine. 
cgcc <- read.table(s3_2_file("cgcc",0))
folders <-cgcc[cgcc["V1"] == "PRE"]
rec_builder(folders,"")
