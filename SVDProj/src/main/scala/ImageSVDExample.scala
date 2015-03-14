
import java.io._
import javax.imageio._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.linalg._

// imgData( x*w + y )
object ImageSVDExample{

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("SVD");
      val sc = new SparkContext(conf);
      var img = ImageIO.read(new File("abc.png"));
      var k = 10;
      var row  = img.getHeight;
      var col = img.getWidth;
      val imgData = Array.ofDim[Double](row*col);
      for( y <- 0 to (row-1); x <- 0 to (col-1) ){
        imgData(x*row+y) = img.getRGB(x, y) & 0x000000ff; // because it is a grey scale , any of RGB will be enough to store   
      }
      //DenseMatrix dm = new DenseMatrix(row,col,imgData);
      val svd.SVD(u,s,vt) = svd(new DenseMatrix(row,col,imgData)); // this should return me the SVD as 3 different matrix
      
      val reconImg = u(::,0 to  k) * diag(s(0 to k)) * vt(0 to k , ::);
      
  }
}
