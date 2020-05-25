//practice scala

//Loops
val players = List("Dhoni","Jaddu")
for(i<-players) println(i)
	(or)
players.foreach(println)

//Map loop
val skill = Map("Dhoni" -> "Bat","Jaddu" -> "Bowl")
for((i,j)<-skill) println(s"Name:${i} Role:${j}")
	(or)
skill.foreach{case(name,role) => println(s"Name:${name} Role:${role}")}

//for expression,yield(assigning for loop results to new val)
val num = Seq(1,2,3)
val doubles = for(i<-num) yield i*2
val names = List("@gopi","_Nath","$Arul")
val capName = for(i<-names) yield i.drop(1).capitalize

//match in scala
val getSkill = i match{
	case "Dhoni" => "Bat"
	case "Tahir" => "Bowl"
	case _ => "No player exists"
}

//function with match
def getPlayerSkill(name:String):String = name match{
	case "Dhoni" => "Bat"
	case "Tahir" => "Bowl"
    case _ => "No"}

val skill = getPlayerSkill("Dhoni")

def isTrue(a:Any) = a match{
	case 0 | "" => false
	case _ => true
}

//simple program using match and functions
object HelloWorld{
   def main(args: Array[String]) {
      def getAdd(a:Int):Unit = {
	    //val b = a+a
	println(s"Addition: ${a+a}")
    }

def getBore(a:Any) = {
	println(a)
}


def getInput(a:Any) = a match{
	case "ADD" => getAdd(2)
	case "Name" => println("Your name is Gopi")
	case x if x == 3 => println("Hey you typed biggest number")
	case _ => getBore(a)
   }
   val ans = getInput("ADD")
   println(ans)
}
    
}

//collections
//ArrayBuffer -- Mutable
import scala.collection.mutable.ArrayBuffer
val a = new ArrayBuffer[Int]()
a += 5
a += List(1,2,3,5)
a += 7 += 8
val a = ArrayBuffer(1,2,3,4,5)
a.append(7) //ArrayBuffer(1,2,3,4,5,7)
a.insert(0,10) //ArrayBuffer(10,1,2,3,4,5,7)
a.insertAll(0,List(88,99)) //ArrayBuffer(88,99,.....)
a.prepend(1,2)
val b = ArrayBuffer.range('a','e')//ArrayBuffer('a','b','c','d')

//List --Immutable
val a = List(1,2,3)
val b = List(4,5) ++: a //List(4,5,1,2,3)

//Vector --Immutable same like list but they are indexed
val a = Vector('Gopi','Nath')

//Map -- Both mutable and immutable
import scala.collection.mutable.Map
val skill = collection.mutable.Map(1->"Dh")
skill += (2->"Jaddu")
skill ++= Map(3->"Viru",4->"Tahir")
val m = Map(1->"Gopi",2->"Nath")
val keys = m.key //Set(1,2)
val values = m.value //HashMap("Gopi","Nath")
val upper = m.transform((k,v))
val filtering = m.view.filterKeys(Set(2)).toMap // 2->"Jaddu"

//.map,.filter FP
val num = List.range(1,10) //List(1,2,3,4,5,6,7,8,9)
val even = num.filter(_%2==0) (or)  val even = num.filter(i=>i%2==0)
val doubles = num.map(_*2) //List(1,4,9,.....)
val newnum = List(1,2,4)
val doublednewnum = newnum.map(doubles) //here doubles val acting as a function, same like below def(Taste of complete FP)
def doubles(a:Int):Int = {a*2}

//take,takeWhile,dropWHile,reduce
val doubl = List(1,2,3,4,5,6,7)
doubl.takeWhile(_<5)
doubl.dropWhile(_ > 5)
	
//reduce -- combines the values perform operation untill it reduces to single values
def add(a:Int,b:Int) : Int = {a+b}
val a = (1 to 10).toList
val added = a.reduce(add) //here map wont work,argument erros
val added = a.reduce(_+_) //same can be written like this

//getting table fields dynamically

val m = Map("md_customer"->Array[string]("custid","custDesc"),"md_product"->Array[String]("prdid","productType","Flag"),
        "md_uom"->Array[String]("prdid","uom","conversion")) //mapped tables along with fields

def getSchema(table:String):StructType = {
    val struct = new StructType()
    m(table).foreach(ele => {struct.add(StructField(ele,StringType,true))})	
	struct
}

//Anonymous functions
def genericOper[T:>Any](f:(T,T)=>T,op1:T,op2:T):T = f(op1,op2)
genericOper[Float]((x,y) => x*y,2.5f,3.5f)

//getFix
def getFix(a:String,b:String) : String ={
	if(a != "X0") {return s"Change MRP type from ${a} to XO "}
	if(b == "X")   {return s"Change deletion indicator from X to Null"}
	
}

val df_Final = df.withColumn("Action",getFix(df.col("MRP"),df.col("Deletion")))






	




