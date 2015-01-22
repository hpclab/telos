package it.unipi.telos.util

class TelosPropertiesImmutable(	 
								val appName : String,
								val dataset : String, 
								val jarPath : String, 
								val maxStep : Int,
								val sparkMaster : String,
								val checkpointDir : String,
								val sparkPartition : Int,
								val sparkExecutorMemory : String, 
								val sparkBlockManagerSlaveTimeoutMs : String,
								val sparkCoresMax : Int,
								val separator : String,
								val jabejaPartition : Int,
								val jabejaSeedColoring : Int,
								val jabejaSeedProtocol : Int,
								val jabejaAlpha : Double,
								val jabejaSaStart : Double,
								val jabejaSaDelta : Double,
								val jabejaRandomViewSize : Int,
								val jabejaPartnerPickOrder : String,
								val jabejaPartnerProbaility : Array[Double],
								val jabejaMinCutSamplingRate : Int,
								val jabejaUseTMan : Boolean,
								val jabejaTManStep : Int,
								val jabejaTManC : Int,
								val jabejaTManR : Int,
								val jabejaTManViewSize : Int,
								val jabejaApproximate : Boolean
								) extends Serializable
{

}