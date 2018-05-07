import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import config as conf

def plotTransactionTypes(dataForTransactionTypesPerBlock):	
	private = [d[0] for d in dataForTransactionTypesPerBlock]
	public = [d[1] for d in dataForTransactionTypesPerBlock]
	shielded = [d[2] for d in dataForTransactionTypesPerBlock]
	deshielded = [d[3] for d in dataForTransactionTypesPerBlock]
	mixed = [d[4] for d in dataForTransactionTypesPerBlock]
	coingens = [d[5] for d in dataForTransactionTypesPerBlock]

	time = range(sum(coingens)+1)
	x, y = zip(*zip(time,coingens))
	y = np.array(y).cumsum()

	x, y2= zip(*zip(time,public))
	y2 = np.array(y2).cumsum()

	x, y3= zip(*zip(time,shielded))
	y3 = np.array(y3).cumsum()

	x, y4= zip(*zip(time,deshielded))
	y4 = np.array(y4).cumsum()

	x, y5= zip(*zip(time,mixed))
	y5 = np.array(y5).cumsum()

	x, y6 = zip(*zip(time,private))
	y6 = np.array(y6).cumsum()

	fig, ax = plt.subplots(1)
	Deshielded, = ax.plot(x, y4, 'r-',linewidth=2)
	Coingens, = ax.plot(x, y, 'b-',linewidth=2)
	Pubs, = ax.plot(x, y2, 'g-',linewidth=2)
	Shielded, = ax.plot(x,y3, 'y-',linewidth=2)
	Mixed, = ax.plot(x,y5, 'm-',linewidth=2)
	Private, = ax.plot(x,y6, 'c',linewidth=2)
	locs, labels = plt.xticks()
	ylocs, ylabels = plt.yticks()
	plt.legend([Deshielded,Coingens,Pubs,Shielded,Mixed,Private],["Deshielded","Coingens","Pubs","Shielded","Mixed","Private"], loc='upper left',fontsize=12)	
	ax.set_xlabel("Blockheight",size=14)
	ax.set_ylabel("Total number of transactions",size=14)
	plt.xlim([0,sum(coingens)+1000])
	plt.tight_layout()
	fig.savefig(conf.pathresearch+"Graphs/TransactionTypes.pdf")
	
	return

def plotTotalValueOverTime(dataForTotalValueOverTime):
	
	time=range(len(dataForTotalValueOverTime))
	x, y= zip(*zip(time,dataForTotalValueOverTime))
	
	y = np.array(y).cumsum()

	fig, ax = plt.subplots(1)

	Value, = ax.plot(x, y, 'r-',linewidth=2)

	locs, labels = plt.xticks()
	ylocs, ylabels = plt.yticks()
	ax.set_xlabel("Blockheight",fontsize=14)
	ax.set_ylabel("Total value in the pool ",fontsize=14)
	plt.xlim([0,len(dataForTotalValueOverTime)+1000])
	plt.tight_layout()

	fig.savefig(conf.pathresearch+"Graphs/TotalValueOverTime.pdf")

	return 

def plotDepositsWithdrawals(dataForDepositsWithdrawals):

	time=range(len(dataForDepositsWithdrawals))

	outs=[-d[0] for d in dataForDepositsWithdrawals]
	ins=[d[1] for d in dataForDepositsWithdrawals]
	
	x, y1= zip(*zip(time,ins))
	x, y2= zip(*zip(time,outs))

	fig, ax = plt.subplots(1)
	Ins, = ax.plot(x, y1, 'r-', linewidth=2)
	Outs, = ax.plot(x, y2, 'b-', linewidth=2)

	locs, labels = plt.xticks()
	ylocs, ylabels = plt.yticks()

	plt.legend([Ins,Outs],["Deposits","Withdrawals"], loc='upper left',fontsize=14)

	ax.set_xlabel("Date",size=14)
	ax.set_ylabel("Deposits-Withdrawals in each block",fontsize=14)
	plt.xlim([0,len(dataForDepositsWithdrawals)+1000])
	plt.tight_layout()

	fig.savefig(conf.pathresearch+"Graphs/Deposits-Withdrawals.pdf")

	return

def plotInteractionsPerIdentity(data,miners,founders,interactionType,blockHeight):
	
	time = range(blockHeight)

	m=[0 for _ in range(blockHeight+1)]	
	f=[0 for _ in range(blockHeight+1)]	
	o=[0 for _ in range(blockHeight+1)]	
	
	for row in data:
		if row.a[0] in miners:
			m[row.h]+=row.v
		elif row.a[0] in founders:
			f[row.h]+=row.v
		else:
			o[row.h]+=row.v

	a=[m[index]+f[index]+o[index] for index in range(blockHeight)]

	x, y = zip(*zip(time,m))
	y = np.array(y).cumsum()

	x, y2= zip(*zip(time,f))
	y2 = np.array(y2).cumsum()

	x, y3= zip(*zip(time,o))
	y3 = np.array(y3).cumsum()

	x, y4= zip(*zip(time,a))
	y4 = np.array(y4).cumsum()

	fig, ax = plt.subplots(1)
	All, = ax.plot(x, y4, '#000000',linewidth=2)
	Miners, = ax.plot(x, y, '#ff00ff',linewidth=2)
	Founders, = ax.plot(x, y2, '#00ffff',linewidth=2)
	Others, = ax.plot(x,y3, '#ffa500',linewidth=2)

	locs, labels = plt.xticks()
	ylocs, ylabels = plt.yticks()
	plt.legend([All,Founders,Miners,Others],["All","Founders","Miners","Others"], loc='upper left',fontsize=14)
	ax.set_xlabel("Blockheight", fontsize=14)
	ax.set_ylabel("Total Value",fontsize=14)
	plt.xlim([0,blockHeight+1000])
	plt.tight_layout()
	fig.savefig(conf.pathresearch+"Graphs/"+str(interactionType)+".pdf")

	return 

def correlateFounders(dfvj,dft,dfb,blockheight):

	inputs=dfvj.where(dfvj.vj_vpub_old==249.9999)
	inputs=inputs.join(dft,inputs.vj_tx_hash==dft.t_tx_hash)
	inputs=inputs.join(dfb,inputs.t_blockhash==dfb.b_hash)
	inputs=inputs.selectExpr("b_height as height").collect()
	
	outputs=dfvj.where(dfvj.vj_vpub_new==250.0001)
	outputs=outputs.join(dft,outputs.vj_tx_hash==dft.t_tx_hash)
	outputs=outputs.join(dfb,outputs.t_blockhash==dfb.b_hash)
	outputs=outputs.selectExpr("b_height as height").collect()

	inputs=[row.height for row in inputs]
	outputs=[row.height for row in outputs]

	i=[0 for _ in range(blockheight+1)]
	o=[0 for _ in range(blockheight+1)]
	
	for element in inputs:
		i[element]+=249.9999
	for element in outputs:
		o[element]+=250.0001

	inputs=i
	outputs=o
	
	return inputs,outputs

def plotFoundersCorrelation(inputs,outputs,blockheight):

	time = range(blockheight+1)
	x, y1 = zip(*zip(time,inputs))
	y1 = np.array(y1).cumsum()

	x, y2= zip(*zip(time,outputs))
	y2 = np.array(y2).cumsum()

	fig, ax = plt.subplots(1)
	Inputs, = ax.plot(x, y1, 'r-',linewidth=2)
	Outputs, = ax.plot(x, y2, 'b-',linewidth=2)
	ax.set_xlabel("Blockheight",size=14)
	ax.set_ylabel("Total Value",size=14)
	plt.legend([Inputs,Outputs],["Inputs","Outputs"], loc='upper left',fontsize=14)
	plt.xlim([0,blockheight+1000])
	plt.tight_layout()
	fig.savefig(conf.pathresearch+"Graphs/FounderCorrelation.pdf")

	return
