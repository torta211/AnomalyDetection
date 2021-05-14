from AnomalyDetector import AnomalyDetector


class ContextOperator(object):
    """
    Contextual Anomaly Detector - Open Source Edition
    2016, Mikhail Smirnov   smirmik@gmail.com
    https://github.com/smirmik/CAD
    """

    def __init__(self, maxLeftSemiContextsLenght):

        self.maxLeftSemiContextsLenght = maxLeftSemiContextsLenght

        self.factsDics = [{}, {}]
        self.semiContextDics = [{}, {}]
        self.semiContValLists = [[], []]
        self.crossedSemiContextsLists = [[], []]
        self.contextsValuesList = []

        self.newContextID = False

    def getContextByFacts(self, newContextsList, zerolevel=0):
        """
        The function which determines by the complete facts list whether the
        context is already saved to the memory. If the context is not found the
        function immediately creates such. To optimize speed and volume of the
        occupied memory the contexts are divided into semi-contexts as several
        contexts can contain the same facts set in its left and right parts.

        @param newContextsList:     list of potentially new contexts

        @param zerolevel:         flag indicating the context type in
                        transmitted list

        @return : depending on the type of  potentially new context transmitted as
              an input parameters the function returns either:
              a) flag indicating that the transmitted zero-level context is
              a new/existing one;
              or:
              b) number of the really new contexts that have been saved to the
              context memory.
        """

        numAddedContexts = 0

        for leftFacts, rightFacts in newContextsList:

            leftHash = leftFacts.__hash__()
            rightHash = rightFacts.__hash__()

            nextLeftSemiContextNumber = len(self.semiContextDics[0])
            leftSemiContextID = self.semiContextDics[0].setdefault(
                leftHash,
                nextLeftSemiContextNumber
            )
            if leftSemiContextID == nextLeftSemiContextNumber:
                leftSemiContVal = [[], len(leftFacts), 0, {}]
                self.semiContValLists[0].append(leftSemiContVal)
                for fact in leftFacts:
                    semiContextList = self.factsDics[0].setdefault(fact, [])
                    semiContextList.append(leftSemiContVal)

            nextRightSemiContextNumber = len(self.semiContextDics[1])
            rightSemiContextID = self.semiContextDics[1].setdefault(
                rightHash,
                nextRightSemiContextNumber
            )
            if rightSemiContextID == nextRightSemiContextNumber:
                rightSemiContextValues = [[], len(rightFacts), 0]
                self.semiContValLists[1].append(rightSemiContextValues)
                for fact in rightFacts:
                    semiContextList = self.factsDics[1].setdefault(fact, [])
                    semiContextList.append(rightSemiContextValues)

            nextFreeContextIDNumber = len(self.contextsValuesList)
            contextID = self.semiContValLists[0][leftSemiContextID][3].setdefault(
                rightSemiContextID,
                nextFreeContextIDNumber
            )

            if contextID == nextFreeContextIDNumber:
                numAddedContexts += 1
                contextValues = [0, zerolevel, leftHash, rightHash]

                self.contextsValuesList.append(contextValues)
                if zerolevel:
                    self.newContextID = contextID
                    return True
            else:
                contextValues = self.contextsValuesList[contextID]

                if zerolevel:
                    contextValues[1] = 1
                    return False

        return numAddedContexts

    def contextCrosser(self,
                       leftOrRight,
                       factsList,
                       newContextFlag=False,
                       potentialNewContexts=None):

        if leftOrRight == 0:
            if len(potentialNewContexts) > 0:
                numNewContexts = self.getContextByFacts(potentialNewContexts)
            else:
                numNewContexts = 0

        for semiContextValues in self.crossedSemiContextsLists[leftOrRight]:
            semiContextValues[0] = []
            semiContextValues[2] = 0

        for fact in factsList:
            for semiContextValues in self.factsDics[leftOrRight].get(fact, []):
                semiContextValues[0].append(fact)

        newCrossedValues = []

        for semiContextValues in self.semiContValLists[leftOrRight]:
            lenSemiContextValues0 = len(semiContextValues[0])
            semiContextValues[2] = lenSemiContextValues0
            if lenSemiContextValues0 > 0:
                newCrossedValues.append(semiContextValues)

        self.crossedSemiContextsLists[leftOrRight] = newCrossedValues

        if leftOrRight:
            return self.updateContextsAndGetActive(newContextFlag)

        else:
            return numNewContexts

    def updateContextsAndGetActive(self, newContextFlag):
        """
        This function reviews the list of previously selected left semi-contexts,
        creates the list of potentially new contexts resulted from intersection
        between zero-level contexts, determines the contexts that coincide with
        the input data and require activation.

        @param newContextFlag:     flag indicating that a new zero-level
                        context is not recorded at the current
                        step, which means that all contexts
                        already exist and there is no need to
                        create new ones.

        @return activeContexts:     list of identifiers of the contexts which
                        completely coincide with the input stream,
                        should be considered active and be
                        recorded to the input stream of "neurons"

        @return potentialNewContextsLists:  list of contexts based on intersection
                        between the left and the right zero-level
                        semi-contexts, which are potentially new
                        contexts requiring saving to the context
                        memory
        """

        activeContexts = []
        numSelectedContext = 0

        potentialNewContexts = []

        for leftSemiContVal in self.crossedSemiContextsLists[0]:

            for rightSemiContextID, contextID in leftSemiContVal[3].items():

                if self.newContextID != contextID:

                    contextValues = self.contextsValuesList[contextID]
                    rightSemiContVal = self.semiContValLists[1][rightSemiContextID]
                    rightSemConVal0, rightSemConVal1, rightSemConVal2 = rightSemiContVal

                    if leftSemiContVal[1] == leftSemiContVal[2]:

                        numSelectedContext += 1

                        if rightSemConVal2 > 0:

                            if rightSemConVal1 == rightSemConVal2:
                                contextValues[0] += 1
                                activeContexts.append([contextID,
                                                       contextValues[0],
                                                       contextValues[2],
                                                       contextValues[3]
                                                       ])

                            elif contextValues[1] and newContextFlag:
                                if leftSemiContVal[2] <= self.maxLeftSemiContextsLenght:
                                    leftFacts = tuple(leftSemiContVal[0])
                                    rightFacts = tuple(rightSemConVal0)
                                    potentialNewContexts.append(tuple([leftFacts, rightFacts]))

                    elif contextValues[1] and newContextFlag and rightSemConVal2 > 0:
                        if leftSemiContVal[2] <= self.maxLeftSemiContextsLenght:
                            leftFacts = tuple(leftSemiContVal[0])
                            rightFacts = tuple(rightSemConVal0)
                            potentialNewContexts.append(tuple([leftFacts, rightFacts]))

        self.newContextID = False

        return activeContexts, numSelectedContext, potentialNewContexts



class ContextualAnomalyDetectorOSE(object):
    """
    Contextual Anomaly Detector - Open Source Edition
    2016, Mikhail Smirnov   smirmik@gmail.com
    https://github.com/smirmik/CAD
    """

    def __init__(self,
                 minValue,
                 maxValue,
                 baseThreshold = 0.75,
                 restPeriod = 30,
                 maxLeftSemiContextsLenght = 7,
                 maxActiveNeuronsNum = 15,
                 numNormValueBits = 3 ):

        self.minValue = float(minValue)
        self.maxValue = float(maxValue)
        self.restPeriod = restPeriod
        self.baseThreshold = baseThreshold
        self.maxActNeurons = maxActiveNeuronsNum
        self.numNormValueBits = numNormValueBits

        self.maxBinValue = 2 ** self.numNormValueBits - 1.0
        self.fullValueRange = self.maxValue - self.minValue
        if self.fullValueRange == 0.0:
            self.fullValueRange = self.maxBinValue
        self.minValueStep = self.fullValueRange / self.maxBinValue

        self.leftFactsGroup = tuple()

        self.contextOperator = ContextOperator(maxLeftSemiContextsLenght)

        self.potentialNewContexts = []

        self.aScoresHistory = [1.0]


    def step(self, inpFacts):
        currSensFacts = tuple(sorted(set(inpFacts)))

        uniqPotNewContexts = set()

        if len(self.leftFactsGroup) > 0 and len(currSensFacts) > 0:
            potNewZeroLevelContext = tuple([self.leftFactsGroup, currSensFacts])
            uniqPotNewContexts.add(potNewZeroLevelContext)
            newContextFlag = self.contextOperator.getContextByFacts(
                [potNewZeroLevelContext],
                zerolevel=1)
        else:
            newContextFlag = False

        leftCrossing = self.contextOperator.contextCrosser(
            leftOrRight=1,
            factsList=currSensFacts,
            newContextFlag=newContextFlag)
        activeContexts, numSelContexts, potNewContexts = leftCrossing

        uniqPotNewContexts.update(potNewContexts)
        numUniqPotNewContext = len(uniqPotNewContexts)

        if numSelContexts > 0:
            percentSelectedContextActive = len(activeContexts) / float(numSelContexts)
        else:
            percentSelectedContextActive = 0.0

        srtAContexts = sorted(activeContexts, key=lambda x: (x[1], x[2], x[3]))
        activeNeurons = [cInf[0] for cInf in srtAContexts[-self.maxActNeurons:]]

        currNeurFacts = set([2 ** 31 + fact for fact in activeNeurons])

        leftFactsGroup = set()
        leftFactsGroup.update(currSensFacts, currNeurFacts)
        self.leftFactsGroup = tuple(sorted(leftFactsGroup))

        numNewCont = self.contextOperator.contextCrosser(
            leftOrRight=0,
            factsList=self.leftFactsGroup,
            potentialNewContexts=potNewContexts)

        numNewCont += 1 if newContextFlag else 0

        if newContextFlag and numUniqPotNewContext > 0:
            percentAddedContextToUniqPotNew = numNewCont / float(numUniqPotNewContext)
        else:
            percentAddedContextToUniqPotNew = 0.0

        return percentSelectedContextActive, percentAddedContextToUniqPotNew

    def getAnomalyScore(self, inputData):
        normInpVal = int((inputData["value"] - self.minValue) / self.minValueStep)
        binInpValue = bin(normInpVal).lstrip("0b").rjust(self.numNormValueBits, "0")

        outSens = []
        for sNum, currSymb in enumerate(reversed(binInpValue)):
            outSens.append(sNum * 2 + (1 if currSymb == "1" else 0))
        setOutSens = set(outSens)

        anomalyVal1, anomalyVal2 = self.step(setOutSens)
        currentAnomalyScore = (1.0 - anomalyVal1 + anomalyVal2) / 2.0

        if max(self.aScoresHistory[-int(self.restPeriod):]) < self.baseThreshold:
            returnedAnomalyScore = currentAnomalyScore
        else:
            returnedAnomalyScore = 0.0

        self.aScoresHistory.append(currentAnomalyScore)

        return returnedAnomalyScore


class ContextOSEDetector(AnomalyDetector):
    """
    This detector uses Contextual Anomaly Detector - Open Source Edition
    2016, Mikhail Smirnov   smirmik@gmail.com
    https://github.com/smirmik/CAD
    """

    def __init__(self, *args, **kwargs):
        super(ContextOSEDetector, self).__init__(*args, **kwargs)

        self.cadose = None

    def handle_record(self, inputData):
        anomalyScore = self.cadose.getAnomalyScore(inputData)
        return (anomalyScore,)

    def initialize(self):
        self.cadose = ContextualAnomalyDetectorOSE(
            minValue=self.input_min,
            maxValue=self.input_max,
            restPeriod=self.probationary_period / 5.0,
        )
