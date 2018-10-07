data <- read.table("D:/features-sub1.csv", header = TRUE, sep = ",")

data[is.na(data)] <- 0

writeInLibSvm <- function(df, f)
{
  x <- as.matrix(df[,2:23])
  
  # put the labels in a separate vector
  y <- df[,1]
  
  # convert to compressed sparse row format
  xsparse <- as.matrix.csr(x)
  
  # write the output libsvm format file 
  write.matrix.csr(xsparse, y=y, file=f)
}

writeInLibSvm(data, "D:/libsvm_features-sub1.dat")