rm(list=ls())
setwd("C:/Users/yutao/OneDrive/Desktop/GradSchool/2023 Winter/MACS30122 Compuational Methods/final-project-internacia/data")

Param_1 <- read.csv("Param_1.csv")
Std_Error_1 <- read.csv("Std_Err_1.csv")
Param_2 <- read.csv("Param_2.csv")
Std_Error_2 <- read.csv("Std_Err_2.csv")

Param_1$PC1_Upper <- Param_1$PC1 + Std_Error_1$PC1*1.96
Param_1$PC1_Lower <- Param_1$PC1 - Std_Error_1$PC1*1.96

Param_1$PC2_Upper <- Param_1$PC2 + Std_Error_1$PC2*1.96
Param_1$PC2_Lower <- Param_1$PC2 - Std_Error_1$PC2*1.96

Param_1$CINC_Upper <- Param_1$CINC + Std_Error_1$Political*1.96
Param_1$CINC_Lower <- Param_1$CINC - Std_Error_1$Political*1.96

Param_1$Economics_Upper <- Param_1$Economics + Std_Error_1$Economics*1.96
Param_1$Economics_Lower <- Param_1$Economics - Std_Error_1$Economics*1.96

Param_2$PC1_Upper <- Param_2$PC1 + Std_Error_2$PC1*1.96
Param_2$PC1_Lower <- Param_2$PC1 - Std_Error_2$PC1*1.96

Param_2$CINC_Upper <- Param_2$Politics + Std_Error_2$Politics*1.96
Param_2$CINC_Lower <- Param_2$Politics - Std_Error_2$Politics*1.96

Param_2$Economics_Upper <- Param_2$Economics + Std_Error_2$Economics*1.96
Param_2$Economics_Lower <- Param_2$Economics - Std_Error_2$Economics*1.96

library(ggplot2)
ggplot()+geom_ribbon(data=Param_1,mapping=aes(x=Year,ymin=PC1_Lower,ymax=PC1_Upper),alpha=0.2)+
  geom_line(data=Param_1,mapping=aes(x=Year,y=PC1,color="International Relationship (PC1)"))

ggplot()+geom_ribbon(data=Param_1,mapping=aes(x=Year,ymin=PC2_Lower,ymax=PC2_Upper),alpha=0.2)+
  geom_line(data=Param_1,mapping=aes(x=Year,y=PC2,color="International Relationship (PC2)"))

ggplot()+geom_ribbon(data=Param_1,mapping=aes(x=Year,ymin=CINC_Lower,ymax=CINC_Upper),alpha=0.2)+
  geom_line(data=Param_1,mapping=aes(x=Year,y=CINC,color="Politics"))

ggplot()+geom_ribbon(data=Param_1,mapping=aes(x=Year,ymin=Economics_Lower,ymax=Economics_Upper),alpha=0.2)+
  geom_line(data=Param_1,mapping=aes(x=Year,y=Economics,color="Economics"))

ggplot()+geom_ribbon(data=Param_1,mapping=aes(x=Year,ymin=PC1_Lower,ymax=PC1_Upper),alpha=0.2)+
  geom_line(data=Param_1,mapping=aes(x=Year,y=PC1,color="International Relationship (PC1)"))

ggplot()+geom_ribbon(data=Param_2,mapping=aes(x=Year,ymin=CINC_Lower,ymax=CINC_Upper),alpha=0.2)+
  geom_line(data=Param_2,mapping=aes(x=Year,y=Politics,color="Politics"))

ggplot()+geom_ribbon(data=Param_2,mapping=aes(x=Year,ymin=Economics_Lower,ymax=Economics_Upper),alpha=0.2)+
  geom_line(data=Param_2,mapping=aes(x=Year,y=Economics,color="Economics"))
