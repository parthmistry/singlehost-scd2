package post.parthmistry.singlehostscd2.service

import post.parthmistry.singlehostscd2.data.ProductDim

import java.sql.Timestamp
import java.text.DecimalFormat
import scala.util.Random

object DataGeneratorService {

  private val productIdGenerators = List(
    new BasicIdGenerator(),
    new LongIdGenerator()
  )

  private val brands = List("brand1", "brand2", "brand3", "brand4")

  private val categories = List("category1", "category2", "category3", "category4", "category5", "category6", "category7")

  private val colors = List("red", "white", "black")

  private val decimalFormat = new DecimalFormat("#.##")

  private val randomGenerator = new Random()

  def generateRecords(startId: Int, count: Int, effStartDate: Timestamp): List[ProductDim] = {
    (0 until count).map(i => {
      generateRecord(startId + i, effStartDate)
    }).toList
  }

  def generateRecord(currentNum: Int, effStartDate: Timestamp): ProductDim = {
    val id = productIdGenerators(currentNum % productIdGenerators.size).generateId(currentNum)
    val randomValue = randomGenerator.nextInt(10)

    ProductDim(
      id = id,
      name = "name_" + currentNum,
      brand = brands(randomValue % brands.size),
      description = "sample product description " + currentNum,
      category = categories(randomValue % categories.size),
      price = BigDecimal(decimalFormat.format(randomGenerator.nextDouble() * 150)),
      color = colors(currentNum % colors.size),
      weight = 150 + randomGenerator.nextInt(100),
      imageUrl = "https://sample/image/url/" + currentNum,
      customAttribute = "attr_value_" + currentNum,
      isCurrent = true,
      effStartDate = effStartDate,
      effEndDate = null
    )
  }

}
