library(anchors)
library(zoo)
library(dplyr)
library(Hmisc)
library(plyr)
library(BBmisc)
library(e1071)
library(SparseM)

loadData <- function(fileName)
{
  data <- read.table(fileName, header = FALSE, sep = " ")
  data_filtered <- data[!(data$V2=="0"),]
  
  # colnames(data_filtered) <- c("timeStamp","activityID","heartRate","handTemp","handAcc16x", "handAcc16y", "handAcc16z", "handAcc6x", "handAcc6y", "handAcc6z", "handGyrox", "handGyroy", "handGyroz", "handMagx", "handMagy", "handMagz", "handOrx", "handOry", "handOrz",
  #                              "chestTemp","chestAcc16x", "chestAcc16y", "chestAcc16z", "chestAcc6x", "chestAcc6y", "chestAcc6z", "chestGyrox", "chestGyroy", "chestGyroz", "chestMagx", "chestMagy", "chestMagz", "chestOrx", "chestOry", "chestOrz",
  #                              "ankleTemp","ankleAcc16x", "ankleAcc16y", "ankleAcc16z", "ankleAcc6x", "ankleAcc6y", "ankleAcc6z", "ankleGyrox", "ankleGyroy", "ankleGyroz", "ankleMagx", "ankleMagy", "ankleMagz", "ankleOrx", "ankleOry", "ankleOrz")
  # 
  data_cols_filtered <- data_filtered %>% do(na.locf(.))
  
  return(data_cols_filtered)
}

file1.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject101.dat")
write.table(file1.data, file ="D:/data_for_spark/sub1.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file2.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject102.dat")
write.table(file2.data, file ="D:/data_for_spark/sub2.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file3.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject103.dat")
write.table(file3.data, file ="D:/data_for_spark/sub3.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file4.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject104.dat")
write.table(file4.data, file ="D:/data_for_spark/sub4.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file5.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject105.dat")
write.table(file5.data, file ="D:/data_for_spark/sub5.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file6.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject106.dat")
write.table(file6.data, file ="D:/data_for_spark/sub6.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file7.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject107.dat")
write.table(file7.data, file ="D:/data_for_spark/sub7.txt" ,row.names = FALSE, col.names = FALSE, sep=",")

file8.data <- loadData("D:/PAMAP2_Dataset/PAMAP2_Dataset/Protocol/subject108.dat")
write.table(file8.data, file ="D:/data_for_spark/sub8.txt" ,row.names = FALSE, col.names = FALSE, sep=",")