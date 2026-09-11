package org.dataprov.dp.wringlet

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.ArrayAggregate
import org.apache.spark.sql.catalyst.expressions.ArrayDistinct
import org.apache.spark.sql.catalyst.expressions.ArrayTransform
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
import org.apache.spark.sql.catalyst.expressions.LambdaFunction
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedLambdaVariable
import org.apache.spark.sql.catalyst.expressions.Size
import org.apache.spark.sql.catalyst.expressions.ZipWith
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
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
  def formatWitnessArray(witnessArray: Expression): Expression
}

trait WindowRawProvenanceOperation {
  def windowRaw(attr: Attribute): Expression
}

trait WindowFinalizeProvenanceOperation {
  def windowFinalize(attr: Attribute): Expression
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
    with WindowRawProvenanceOperation
    with WindowFinalizeProvenanceOperation

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
      aggregateFn: Attribute => Expression,
      formatWitnessArrayFn: Expression => Expression
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

    override def formatWitnessArray(witnessArray: Expression): Expression =
      formatWitnessArrayFn(witnessArray)
    override def windowRaw(attr: Attribute): Expression =
      aggregateFn(attr)
    override def windowFinalize(attr: Attribute): Expression =
      attr
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
      aggregateFn = overrides.aggregateFrom.getOrElse(base).aggregate,
      formatWitnessArrayFn =
        overrides.aggregateFrom.getOrElse(base).formatWitnessArray
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

  override def formatWitnessArray(witnessArray: Expression): Expression = {
    val sep = Literal(aggregateOperator)
    val joinedArray = ConcatWs(Seq(sep, witnessArray))
    val withBraces = Concat(Seq(Literal("{"), joinedArray, Literal("}")))

    If(
      GreaterThan(Size(witnessArray), Literal(1)),
      withBraces,
      joinedArray
    )
  }

  override def windowRaw(attr: Attribute): Expression =
    AggregateExpression(
      CollectSet(Cast(attr, StringType)),
      Complete,
      isDistinct = false
    )

  override def windowFinalize(attr: Attribute): Expression = {
    val joinedArray = ConcatWs(Seq(Literal(aggregateOperator), attr))
    val withBraces = Concat(Seq(Literal("{"), joinedArray, Literal("}")))

    If(
      GreaterThan(Size(attr), Literal(1)),
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

  override def formatWitnessArray(witnessArray: Expression): Expression = {
    GreaterThan(Size(witnessArray), Literal(0))
  }

  override def windowRaw(attr: Attribute): Expression =
    AggregateExpression(
      Max(Cast(toBool(attr), IntegerType)),
      Complete,
      isDistinct = false
    )

  override def windowFinalize(attr: Attribute): Expression =
    Coalesce(
      Seq(
        GreaterThan(Cast(attr, IntegerType), Literal(0)),
        Literal(false)
      )
    )
}

// Semi Why-provenance builder: explicit alias over witness-set semantics.
// The provenance is represented as array<string>.
// This is useful when consumers prefer structured provenance tags over display strings.
// The exact result will be preserved for single, join and aggregate operations, but distinct
// will not distinguish between multiple rows contributing to the same output row,
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

    val collectListExpr = AggregateExpression(
      CollectList(toArray(attr)),
      Complete,
      isDistinct = false
    )
    ArrayDistinct(Flatten(collectListExpr))
  }

  override def formatWitnessArray(witnessArray: Expression): Expression = {
    witnessArray
  }

  override def windowRaw(attr: Attribute): Expression =
    AggregateExpression(
      CollectList(toArray(attr)),
      Complete,
      isDistinct = false
    )

  override def windowFinalize(attr: Attribute): Expression =
    ArrayDistinct(Flatten(attr))
}

// Full Why-provenance builder: tracks conjunctions and choices explicitly.
// The provenance is represented as array<array<string>>.
// Each outer element is one factor in the provenance formula.
// Each inner array contains the alternatives for that factor.
// Example: [[a0, a2], [a1], [b1]] encodes (a0 ⊕ a2) ⊗ a1 ⊗ b1.
object FullWhyProvenanceBuilder extends ProvenanceBuilder {
  private val choiceGroupType = ArrayType(StringType, containsNull = true)
  private val factorizedProvenanceType =
    ArrayType(choiceGroupType, containsNull = true)

  // The provenance type is array<array<string>>, where the outer array represents conjunction (AND)
  // and the inner arrays represent disjunction (OR) of alternatives for each factor.
  override val provType: DataType = factorizedProvenanceType

  // For a single input row, the provenance is represented as a single factor with a single choice,
  // i.e. [[tag]]
  private def singletonFactor(choice: Expression): Expression =
    CreateArray(Seq(CreateArray(Seq(choice))))

  // An empty provenance is represented as an empty array of factors: []
  private def emptyFactorizedProvenance: Expression =
    Literal.create(Seq.empty, factorizedProvenanceType)

  // Normalize the provenance attribute to the expected format of array<array<string>>.
  private def normalizeToChoiceFactors(attr: Expression): Expression = {
    attr.dataType match {
      case ArrayType(ArrayType(StringType, _), _) =>
        attr
      case ArrayType(StringType, _) =>
        val elem =
          NamedLambdaVariable("fullWhyElement", StringType, nullable = true)
        ArrayTransform(
          attr,
          LambdaFunction(CreateArray(Seq(elem)), Seq(elem))
        )
      case _ =>
        If(
          IsNull(attr),
          Literal.create(null, factorizedProvenanceType),
          singletonFactor(Cast(attr, StringType))
        )
    }
  }
  // For JOIN, we combine factors from left and right, and merge alternatives within each factor with union.
  private def mergeAlternativesByPosition(
      left: Expression,
      right: Expression
  ): Expression = {
    val leftChoice =
      NamedLambdaVariable("leftChoice", choiceGroupType, nullable = true)
    val rightChoice =
      NamedLambdaVariable("rightChoice", choiceGroupType, nullable = true)

    // For each position, we take the union of alternatives from left and right
    ZipWith(
      left,
      right,
      // If both sides have alternatives, merge them with union.
      // If only one side has alternatives, take those.
      LambdaFunction(
        If(
          And(IsNotNull(leftChoice), IsNotNull(rightChoice)),
          ArrayDistinct(Concat(Seq(leftChoice, rightChoice))),
          Coalesce(Seq(leftChoice, rightChoice))
        ),
        Seq(leftChoice, rightChoice)
      )
    )
  }
  // For DISTINCT / DEDUPLICATE, we take the set of all alternatives for each factor across rows in the group.
  private def collectDistinctRows(attr: Attribute): Expression =
    AggregateExpression(
      CollectSet(normalizeToChoiceFactors(attr)),
      Complete,
      isDistinct = false
    )

  // For a single input row, we normalize the provenance attribute to the expected format of array<array<string>>.
  override def single(attr: Attribute): Expression =
    normalizeToChoiceFactors(attr)

  // For JOIN, we combine factors from left and right with AND, and merge alternatives within each factor with OR.
  override def join(
      left: Attribute,
      right: Attribute
  ): Expression = {
    // JOIN preserves conjunction: concatenate the factors from both sides.
    val leftFactors = normalizeToChoiceFactors(left)
    val rightFactors = normalizeToChoiceFactors(right)
    val merged = ArrayDistinct(Concat(Seq(leftFactors, rightFactors)))

    Coalesce(Seq(merged, leftFactors, rightFactors))
  }

  override def distinct(attr: Attribute): Expression = {
    // DISTINCT preserves factor positions but merges alternatives inside each factor.
    val collectedChoices = collectDistinctRows(attr)
    val accChoices =
      NamedLambdaVariable(
        "accChoices",
        factorizedProvenanceType,
        nullable = true
      )
    val rowChoices =
      NamedLambdaVariable(
        "rowChoices",
        factorizedProvenanceType,
        nullable = true
      )
    val finishedChoices =
      NamedLambdaVariable(
        "finishedChoices",
        factorizedProvenanceType,
        nullable = true
      )

    ArrayAggregate(
      collectedChoices,
      emptyFactorizedProvenance,
      LambdaFunction(
        mergeAlternativesByPosition(accChoices, rowChoices),
        Seq(accChoices, rowChoices)
      ),
      LambdaFunction(finishedChoices, Seq(finishedChoices))
    )
  }

  override def aggregate(attr: Attribute): Expression = {
    // GROUP BY preserves conjunction and unions all contributing factors.
    val collectedFactors = collectDistinctRows(attr)

    ArrayDistinct(Flatten(collectedFactors))
  }

  override def formatWitnessArray(witnessArray: Expression): Expression = {
    CreateArray(Seq(witnessArray))
  }

  override def windowRaw(attr: Attribute): Expression =
    AggregateExpression(
      CollectSet(normalizeToChoiceFactors(attr)),
      Complete,
      isDistinct = false
    )

  override def windowFinalize(attr: Attribute): Expression =
    ArrayDistinct(Flatten(attr))
}
