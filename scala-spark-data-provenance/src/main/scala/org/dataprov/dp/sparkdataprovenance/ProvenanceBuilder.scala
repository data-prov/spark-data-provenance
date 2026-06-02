package org.dataprov.dp

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.ArrayDistinct
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Flatten
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Size
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

trait ProvenanceOutput {
  def provType: DataType
}

trait SingleProvenanceOperation {
  def single(attr: Attribute): Expression
}

trait JoinProvenanceOperation {
  def join(left: Attribute, right: Attribute): Expression
}

trait DistinctProvenanceOperation {
  def distinct(attr: Attribute): Expression
}

trait AggregateProvenanceOperation {
  def aggregate(attr: Attribute): Expression
}

// Encapsulates how provenance values are represented and combined for joins, distinct
// and group by operations. This trait allows users to customize the representation of
// provenance information, e.g. by using structured types instead of strings
trait ProvenanceBuilder
    extends ProvenanceOutput
    with SingleProvenanceOperation
    with JoinProvenanceOperation
    with DistinctProvenanceOperation
    with AggregateProvenanceOperation

// Helper case class to specify overrides when building a new ProvenanceBuilder from a base.
object ProvenanceBuilder {
  final case class Overrides(
      provTypeFrom: Option[ProvenanceOutput] = None,
      singleFrom: Option[SingleProvenanceOperation] = None,
      joinFrom: Option[JoinProvenanceOperation] = None,
      distinctFrom: Option[DistinctProvenanceOperation] = None,
      aggregateFrom: Option[AggregateProvenanceOperation] = None
  )

  // Build a ProvenanceBuilder from individual functions for each capability.
  def compose(
      provTypeFn: DataType,
      singleFn: Attribute => Expression,
      joinFn: (Attribute, Attribute) => Expression,
      distinctFn: Attribute => Expression,
      aggregateFn: Attribute => Expression
  ): ProvenanceBuilder = new ProvenanceBuilder {
    override val provType: DataType = provTypeFn
    override def single(attr: Attribute): Expression =
      singleFn(attr)
    override def join(left: Attribute, right: Attribute): Expression =
      joinFn(left, right)
    override def distinct(attr: Attribute): Expression =
      distinctFn(attr)
    override def aggregate(attr: Attribute): Expression =
      aggregateFn(attr)
  }

  // Build from a base builder and override only the capabilities you need.
  // This keeps composition flexible without creating many one-off helper methods.
  def withOverrides(
      base: ProvenanceBuilder,
      overrides: Overrides = Overrides()
  ): ProvenanceBuilder =
    compose(
      provTypeFn = overrides.provTypeFrom.getOrElse(base).provType,
      singleFn = overrides.singleFrom.getOrElse(base).single,
      joinFn = overrides.joinFrom.getOrElse(base).join,
      distinctFn = overrides.distinctFrom.getOrElse(base).distinct,
      aggregateFn = overrides.aggregateFrom.getOrElse(base).aggregate
    )
}

// Default builder: provenance with display-oriented String representations.
// This builder produces human-readable provenance expressions, which are useful for debugging and exploration.
object DisplayStringProvenanceBuilder extends ProvenanceBuilder {

  def joinOperator: String = " ⊗ "
  def distinctOperator: String = " ⊕ "
  def aggregateOperator: String = " ⊕ "
  override val provType: DataType = StringType
  // For a single input row, the provenance is represented as
  // the string representation of the provenance attribute
  override def single(attr: Attribute): Expression = Cast(attr, StringType)

  // For JOIN, we combine left and right provenance with the join operator
  // in between, and wrap with parentheses
  override def join(
      left: Attribute,
      right: Attribute
  ): Expression = {
    val leftCast = Cast(left, StringType)
    val rightCast = Cast(right, StringType)

    val matchedTag = Cast(
      If(
        And(IsNotNull(left), IsNotNull(right)),
        Concat(
          Seq(
            Literal("("),
            leftCast,
            Literal(joinOperator),
            rightCast,
            Literal(")")
          )
        ),
        Cast(Literal(null), StringType)
      ),
      StringType
    )

    // Coalesce.nullable is true only when all children are nullable.
    // Wrap casts to force nullable=true at type level while preserving values.
    val leftNullable = If(IsNull(left), Literal(null, StringType), leftCast)
    val rightNullable = If(IsNull(right), Literal(null, StringType), rightCast)

    Coalesce(Seq(matchedTag, leftNullable, rightNullable))
  }
  // For DISTINCT / DEDUPLICATE, we combine the provenance of all rows in the group
  // with the aggregate operator in between.
  override def distinct(
      attr: Attribute
  ): Expression = {
    val collectSetExpr = AggregateExpression(
      CollectSet(Cast(attr, StringType)),
      Complete,
      isDistinct = false
    )
    val joinedArray = ConcatWs(Seq(Literal(distinctOperator), collectSetExpr))
    val withBraces = Concat(Seq(Literal("{"), joinedArray, Literal("}")))

    If(
      GreaterThan(Size(collectSetExpr), Literal(1)),
      withBraces,
      joinedArray
    )
  }
  // For GROUP BY, we take the set of all provenance tags for rows in the group,
  // similar to DISTINCT semantics
  // TODO: we may want to support a different operator for GROUP BY vs DISTINCT
  override def aggregate(
      attr: Attribute
  ): Expression = {
    val collectSetExpr = AggregateExpression(
      CollectSet(Cast(attr, StringType)),
      Complete,
      isDistinct = false
    )
    val joinedArray = ConcatWs(Seq(Literal(aggregateOperator), collectSetExpr))
    val withBraces = Concat(Seq(Literal("{"), joinedArray, Literal("}")))

    If(
      GreaterThan(Size(collectSetExpr), Literal(1)),
      withBraces,
      joinedArray
    )
  }
}

// Boolean builder: provenance is represented as boolean expressions
// that track the presence or absence of input rows.
// This is useful when consumers want to perform further symbolic reasoning
// over provenance information, e.g. for debugging
object BooleanProvenanceBuilder extends ProvenanceBuilder {
  override val provType: DataType = BooleanType

  private def toBool(attr: Attribute): Expression =
    Coalesce(Seq(Cast(attr, BooleanType), Literal(false)))

  override def single(attr: Attribute): Expression =
    toBool(attr)

  override def join(
      left: Attribute,
      right: Attribute
  ): Expression = {
    val lb = toBool(left)
    val rb = toBool(right)
    // inner join: AND; outer join safety fallback
    Coalesce(Seq(And(lb, rb), lb, rb, Literal(false)))
  }

  private def anyTrue(attr: Attribute): Expression = {
    val maxInt = AggregateExpression(
      Max(Cast(toBool(attr), IntegerType)),
      Complete,
      isDistinct = false
    )
    GreaterThan(maxInt, Literal(0))
  }

  override def distinct(attr: Attribute): Expression = anyTrue(attr)

  override def aggregate(attr: Attribute): Expression = anyTrue(attr)
}

// Semi Why-provenance builder: explicit alias over witness-set semantics.
// The provenance is represented as array<string>.
// This is useful when consumers prefer structured provenance tags over display strings.
// The exact result will be preserved for single, join and aggregate operations, but distinct 
//will not distinguish between multiple rows contributing to the same output row,
object SemiWhyProvenanceBuilder extends ProvenanceBuilder {
  private val arrayStringType = ArrayType(StringType, containsNull = true)

  override val provType: DataType = arrayStringType

  private def toArray(attr: Attribute): Expression = {
    attr.dataType match {
      case ArrayType(StringType, _) => attr
      case _ =>
        If(
          IsNull(attr),
          Literal.create(null, arrayStringType),
          CreateArray(Seq(Cast(attr, StringType)))
        )
    }
  }
  // For a single input row, the provenance is represented as a single-element
  // array containing the provenance tag for that row
  override def single(attr: Attribute): Expression = {
    toArray(attr)
  }
  // For JOIN, we take the union of left and right provenance tags,
  // which corresponds to the set of all input rows that contributed to each output row
  override def join(
      left: Attribute,
      right: Attribute
  ): Expression = {
    val leftArray = toArray(left)
    val rightArray = toArray(right)
    val merged = ArrayDistinct(Concat(Seq(leftArray, rightArray)))
    Coalesce(Seq(merged, leftArray, rightArray))
  }
  // For DISTINCT / DEDUPLICATE, we take the set of all provenance tags for rows in the group,
  // which corresponds to the set of all input rows that contributed to each output row in the group
  override def distinct(
      attr: Attribute
  ): Expression = {
    val arrayProv = toArray(attr)
    AggregateExpression(
      MinBy(arrayProv, Size(arrayProv)),
      Complete,
      isDistinct = false
    )
  }
  // For GROUP BY, we take the set of all provenance tags for rows in the group,
  // similar to DISTINCT semantics
  override def aggregate(
      attr: Attribute
  ): Expression = {
    val collectSetExpr = AggregateExpression(
      CollectSet(toArray(attr)),
      Complete,
      isDistinct = false
    )
    ArrayDistinct(Flatten(collectSetExpr))
  }
}

// 
object FullWhyProvenanceBuilder extends ProvenanceBuilder {
  override val provType: DataType = SemiWhyProvenanceBuilder.provType

  override def single(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.single(attr)

  override def join(
      left: Attribute,
      right: Attribute
  ): Expression =
    SemiWhyProvenanceBuilder.join(left, right)

  override def distinct(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.aggregate(attr)

  override def aggregate(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.aggregate(attr)
}


// The aggregate and distinct will not distinguish between multiple rows contributing to the same output row, 
// but the join will still combine left and right provenance tags. This is a more lightweight representation
// that may be sufficient for some use cases.
object LightWhyProvenanceBuilder extends ProvenanceBuilder {
  override val provType: DataType = SemiWhyProvenanceBuilder.provType

  override def single(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.single(attr)

  override def join(
      left: Attribute,
      right: Attribute
  ): Expression =
    SemiWhyProvenanceBuilder.join(left, right)

  override def distinct(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.distinct(attr)

  override def aggregate(attr: Attribute): Expression =
    SemiWhyProvenanceBuilder.distinct(attr)
}


