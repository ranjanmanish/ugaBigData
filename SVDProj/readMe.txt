
Instruction to install 

The directory from where the code to run looks like this structure wise

build.sbt  project  run.sh  src  target  yeast-ribosome-small

where I have created a script run.sh 

which has to be run like 

./run.sh without any argument if the directory are placed like the way depicted above 

the folder yeast-ribosome-small contains all the 1000 image files

the values for different k values 

k = 10  => 240.22378036042062

k = 50  => 151.16086151792624

k = 100 => 97.81990424337322

k= 150 => 66.44451876109497

k =200 => 45.2726893926805

k = 300 => 16.971246835633316


// evaluation time wise / core wise  
master(*) 
// I have given 4 cores to my VM 
Tiem in millisecond = 374823/60 = 374/60 = 6.23 minutes 
# Single core 
master
Time in millisecond = 837065/60 = 83.7/6 = 13.9 minutes 

