import java.io._
import javax.imageio._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.linalg._
import breeze.numerics._
import java.awt.image.BufferedImage
import java.lang.Math
/**
* @author manish ranjan
*/
object ImageSVD{
	
	def readImage(img:BufferedImage): DenseMatrix[Double] = {
			val row  = img.getHeight;
			val col = img.getWidth;
			val imgData = Array.ofDim[Double](row*col);
			for( y <- 0 to (row-1); x <- 0 to (col-1) ){
				imgData(x*col+y) = img.getRGB(x, y) & 0x000000ff; // because it is a grey scale , any of RGB will be enough to store   
			} // bahaa's idea rather than using raster as it was giving unexpected  result 
			new DenseMatrix(row,col,imgData); // return the DenseMatrix
	}

	def computeSVD(imgData:DenseMatrix[Double], k: Int) : DenseMatrix[Double] = {
			val svd.SVD(u,s,vt) = svd(imgData)
			val reconImg = u(::,0 until  k) * diag(s(0 until k)) * vt(0 until k , ::)
			reconImg
			//reconImg
	}


	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("SVD");
			val sc = new SparkContext(conf);
      			val k = 300
			var dir = System.getProperty("user.dir") + "/yeast-ribosome-small";
			val startTime = System.currentTimeMillis();		
			val files = sc.parallelize(new java.io.File(dir).listFiles).filter(_.getName.endsWith(".png")).cache();
			println("I was ever here!!!")
			//println(files.size)
			//files.foreach(println)
      var imdwidSvd = files.map(f => ImageIO.read(f)).map(imdwidoutSvd => readImage(imdwidoutSvd)).map(x => computeSVD(x, k)).reduce(_+_)/files.count().toDouble
      var imdwidoutSvd = files.map(f => ImageIO.read(f)).map(imdwidoutSvd => readImage(imdwidoutSvd)).reduce(_+_)/files.count().toDouble
      var diffMatrix = imdwidoutSvd - imdwidSvd
      val frobNorm = sqrt(trace(diffMatrix*diffMatrix.t)) // here as well I was doing a long calculation , bahaa suggested to use this function of breeze 
      println(frobNorm);
	val estimatedTime = System.currentTimeMillis() - startTime
	println(estimatedTime)
      
	}
}
