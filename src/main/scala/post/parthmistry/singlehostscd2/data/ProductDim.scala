package post.parthmistry.singlehostscd2.data

import java.sql.Timestamp

case class ProductDim(
                       id: String,
                       name: String,
                       brand: String,
                       description: String,
                       category: String,
                       price: BigDecimal,
                       color: String,
                       weight: Int,
                       imageUrl: String,
                       customAttribute: String,
                       isCurrent: Boolean,
                       effStartDate: Timestamp,
                       effEndDate: Timestamp
                     )
