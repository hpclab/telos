package it.unipi.telos.util

import java.util.Properties
import java.io.FileInputStream
import java.io.InputStream


class TelosProperties(algorithmName: String, configurationFile : String) 
{
	val property = new Properties
	
	def load() : TelosProperties =
	{
		var input : InputStream = null
 
		input = new FileInputStream(configurationFile);
 
		property.load(input);
		
		this
	}
	
	def get(data : String, default : String) =
	{
		property.getProperty(data, default)
	}
	
	def getBoolean(data : String, default : Boolean) =
	{
		get(data, default.toString).toBoolean
	}
	
	def getImmutable : TelosPropertiesImmutable =
	{
		val appName = get("appName", "TELOS")
		val dataset = get("dataset", "")
		val jarPath = get("jarPath", "")
		
		val sparkMaster = get("sparkMaster", "local[2]")
		val sparkCheckpointDir = get("sparkCheckpointDir", "/tmp")
		val sparkPartition = get("sparkPartition", "32").toInt
		val sparkCoresMax = get("sparkCoresMax", "-1").toInt
		
		val maxStep = get("maxStep", "100").toInt
		
		val jabejaPartition = get("jabeja.partition", "2").toInt
		val jabejaSeedColoring = get("jabeja.seedColoring", "2000").toInt
		val jabejaSeedProtocol = get("jabeja.seedProtocol", "2000").toInt
		val jabejaAlpha = get("jabeja.alpha", "2").toDouble
		val jabejaSaStart = get("jabeja.saStart", "2").toDouble
		val jabejaSaDelta = get("jabeja.saDelta", "0.0003").toDouble
		val jabejaRandomViewSize = get("jabeja.seedProtocol", "10").toInt
//		case "TMAN_NEIGH_RAND" => PartnerPickOrder.TMAN_NEIGH_RAND
//      case "RAND_TMAN_NEIGH" => PartnerPickOrder.RAND_TMAN_NEIGH
//      case "NEIGH_TMAN_RAND" => PartnerPickOrder.NEIGH_TMAN_RAND
		val jabejaPartnerPickOrder = get("jabeja.partnerPickOrder", "NEIGH_TMAN_RAND")
		val jabejaPartnerProbaility = Array(0.5,0.5)
		val jabejaMinCutSamplingRate = get("jabeja.minCutSamplingRate", "1").toInt
		val jabejaUseTMan = get("jabeja.useTMan", "false").toBoolean
		val jabejaTManStep = get("jabeja.tman.step", "3").toInt
		val jabejaTManC = get("jabeja.tman.c", "10").toInt
		val jabejaTManR = get("jabeja.tman.r", "5").toInt
		val jabejaTManViewSize = get("jabeja.tman.viewSize", "10").toInt
		val jabejaApproximate = get("jabeja.approximate", "true").toBoolean
		
		val sparkExecutorMemory = get("sparkExecutorMemory", "14g")
		val sparkBlockManagerSlaveTimeoutMs= get("sparkBlockManagerSlaveTimeoutMs", "45000")
		
		var separator = get("edgelistSeparator", "space")
		if(separator.equals("space")) separator = " "
		
		new TelosPropertiesImmutable(	appName,
										dataset, 
										jarPath, 
										maxStep, 
										sparkMaster, 
										sparkCheckpointDir,
										sparkPartition, 
										sparkExecutorMemory, 
										sparkBlockManagerSlaveTimeoutMs, 
										sparkCoresMax,
										separator,
										jabejaPartition,
										jabejaSeedColoring,
										jabejaSeedProtocol,
										jabejaAlpha,
										jabejaSaStart,
										jabejaSaDelta,
										jabejaRandomViewSize,
										jabejaPartnerPickOrder,
										jabejaPartnerProbaility,
										jabejaMinCutSamplingRate,
										jabejaUseTMan,
										jabejaTManStep,
										jabejaTManC,
										jabejaTManR,
										jabejaTManViewSize,
										jabejaApproximate)
	}
}