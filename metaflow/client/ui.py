
def display_ui():
    from metaflow import Metaflow, Flow, Run, get_metadata, Step, Task, DataArtifact
    from ipytree import Tree, Node
    import sys
    import os
    import io
    import time
    import ipywidgets as widgets
    import pandas
    import qgrid
    import PIL.Image
    import pprint
    from collections import defaultdict
    import sidecar
    import ipyleaflet
    import geopy
    import ipyleaflet

    hbox = widgets.HBox()
    maxHeight = '800px'
    summaryHBox = widgets.HBox(
        layout=widgets.Layout(width='66%', max_height=maxHeight, overflow_y='auto'))
    divider = widgets.VBox(layout=widgets.Layout(width='3px', margin='1px', height='auto',
                                                 border='1px solid lightgray'))
    dividerV = widgets.HBox(layout=widgets.Layout(height='3px', margin='1px', width='auto',
                                                  border='1px solid lightgray'))
    dividerVArtT = widgets.HBox(layout=widgets.Layout(height='3px', margin='1px', width='auto',
                                                      border='1px solid lightgray'))
    dividerVArtB = widgets.HBox(layout=widgets.Layout(height='3px', margin='1px', width='auto',
                                                      border='1px solid lightgray'))
    pathWidget = widgets.HBox(layout=widgets.Layout(width='auto', margin='1px'))
    tightLayout = widgets.Layout(width='fit-content',
                                 min_width='fit-content',
                                 height='fit-content',
                                 min_height='fit-content')
    # pathElsWidget = widgets.HBox()
    # pathWidget.children += (widgets.Label(value='<Nothing selected>'),pathElsWidget)
    # pathWidget.children = (pathElsWidget,)
    mfPathElNames = ['Flow', 'Run', 'Step', 'Task', 'DataArtifact']
    deriveStepSuccess = True
    alwaysShowFakes = False
    showFlowHistory = True

    # When we refresh flow data we need to reload the right flows
    global _sidecars
    global openedNodes
    global selectedNode
    global selectedNodePathSpec
    _sidecars = []
    openedNodes = dict()
    openedUnfinishedNodes = dict()
    selectedNodePathSpec = ''


    iconMapping = defaultdict(lambda: 'default', {
        'DataArtifact': 'file',
        'Task': 'list-ol',
        'Step': 'list-ol',
        'Run': 'sitemap',
        'Flow': 'sitemap',
        'stdout': 'file-text',
        'stderr': 'file-text',
        'exception': 'exclamation-triangle'
    })

    def iconForNodeName(nodeTypeName):
        return iconMapping[nodeTypeName]

    def iconForNode(mfNode):
        return iconForNodeName(type(mfNode).__name__)

    def iconStyleForNode(mfNode):
        if isinstance(mfNode, Task):
            return "success" if mfNode.successful else "danger"
        elif deriveStepSuccess and isinstance(mfNode, Step):
            successCount = 0
            taskCount = 0
            for task in list(mfNode):
                successCount += 1 if task.successful else 0
                taskCount += 1
            if taskCount == successCount:
                return "success"
            elif successCount == 0:
                return "danger"
            else:
                return "warning"
        elif isinstance(mfNode, Run):
            return "success" if mfNode.successful else "danger"
        else:
            return "default"

    def nameForNode(mfNode):
        if isinstance(mfNode, DataArtifact):
            return mfNode.path_components[-1]
        elif isinstance(mfNode, Task):
            return mfNode.path_components[-1]
        elif isinstance(mfNode, Step):
            return mfNode.path_components[-1]
        elif isinstance(mfNode, Run):
            latestRunStr = ""
            latestRun = mfNode.parent.latest_run
            if latestRun is not None and latestRun.path_components[-1] == \
                    mfNode.path_components[-1]:
                latestRunStr = "(latest)"
            latestSuccessfulRun = mfNode.parent.latest_successful_run
            if latestSuccessfulRun is not None and latestSuccessfulRun.path_components[-1] == \
                    mfNode.path_components[-1]:
                latestRunStr = "(latest successful)"
            stateStr = ""
            if not mfNode.finished and mfNode.successful:
                stateStr = "*"
            return "{0}{1}{2}".format(mfNode.path_components[-1], stateStr, latestRunStr)
        elif isinstance(mfNode, Flow):
            return mfNode.path_components[-1]
        else:
            return "???"

    def dateFromStepNode(node):
        return node['mfObj'].created_at

    def ensureSubNodesFor(node, recursive=False):
        subNodes = node['nodes']
        if subNodes is None:
            subNodes = createNodesFor(node['mfObj'],
                                      parent=node,
                                      recursive=recursive)
            node['nodes'] = subNodes

        return subNodes

    def createNodesFor(mfNode, parent=None, recursive=False):
        subNodes = []

        if isinstance(mfNode, DataArtifact):
            return subNodes

        if isinstance(mfNode, Task):
            if alwaysShowFakes or (mfNode.stdout is not None and len(mfNode.stdout) > 0):
                subNode = {'mfObj': mfNode,
                           'name': 'stdout',
                           'nodes': [],
                           'icon': iconForNodeName('stdout'),
                           'icon_style': "default",
                           'isFakeNode': True,
                           'parent': parent}
                subNodes.append(subNode)
            if alwaysShowFakes or (mfNode.stderr is not None and len(mfNode.stderr) > 0):
                subNode = {'mfObj': mfNode,
                           'name': 'stderr',
                           'nodes': [],
                           'icon': iconForNodeName('stderr'),
                           'icon_style': "default" if alwaysShowFakes else "danger",
                           'isFakeNode': True,
                           'parent': parent}
                subNodes.append(subNode)
            if alwaysShowFakes or mfNode.exception is not None:
                subNode = {'mfObj': mfNode,
                           'name': 'exception',
                           'nodes': [],
                           'icon': iconForNodeName('exception'),
                           'icon_style': "default" if alwaysShowFakes else "danger",
                           'isFakeNode': True,
                           'parent': parent}
                subNodes.append(subNode)

        for node in list(mfNode):
            subSubNodes = None
            if recursive:
                subSubNodes = createNodesFor(node, parent, recursive)
            subNode = {'mfObj': node,
                       'name': nameForNode(node),
                       'nodes': subSubNodes,
                       'icon': iconForNode(node),
                       'icon_style': iconStyleForNode(node),
                       'isFakeNode': False,
                       'parent': parent}
            subNodes.append(subNode)

        if isinstance(mfNode, Run):
            subNodes.sort(key=dateFromStepNode)

        return subNodes

    class DisplayModeDropdown(widgets.Dropdown):
        def __init__(self, out, df, inSidecar, *args, **kwargs):
            super(DisplayModeDropdown, self).__init__(*args, **kwargs)
            self.out = out
            self.df = df
            self.inSidecar = inSidecar

    def displayModeDropdownCallback(event):
        dmd = event['owner']
        dmd.out.clear_output()
        if dmd.value == 'Describe':
            with dmd.out:
                display(dmd.df.describe())
        elif dmd.value == 'Head':
            with dmd.out:
                display(dmd.df)
        elif dmd.value == 'Grid':
            with dmd.out:
                grid = qgrid.show_grid(dmd.df)

                if dmd.inSidecar:
                    grid.layout = widgets.Layout(height=maxHeight)
                else:
                    grid.layout = widgets.Layout(height='auto')

                display(grid)


    class SidecarButton(widgets.Button):
        def __init__(self, sidecarObj, sidecarTitle, *args, **kwargs):
            super(SidecarButton, self).__init__(*args, **kwargs)
            self.sidecarObj = sidecarObj
            self.sidecarTitle = sidecarTitle

        def openSidecar(self) -> None:
            global _sidecars
            sc = sidecar.Sidecar(title=self.sidecarTitle)
            _sidecars.append(sc)

            with sc:
                display(self.sidecarObj if not callable(self.sidecarObj) else self.sidecarObj())

    def sidecarButtonClick(button):
        button.openSidecar()

    def createWidgetForDataArtifact(node, inSidecar=False):
        art = node['mfObj']
        data = art.data
        out = widgets.Output()
        defaultVBox = widgets.VBox(children=[out])

        if isinstance(data, pandas.DataFrame):
            with out:
                display(data)

            exportLabel = widgets.Label(value="Export As:")
            exportName = widgets.Text(placeholder='variable_name')
            exportButton = widgets.Button(description="Export")
            exportWidget = widgets.HBox([exportLabel, exportName, exportButton])
            displayModeDropdown = DisplayModeDropdown(out, data, inSidecar,
                                                      options=['Head', 'Describe', 'Grid'],
                                                      value='Head',
                                                      description="Mode:",
                                                      icon='random',
                                                      layout=tightLayout)
            displayModeDropdown.observe(displayModeDropdownCallback, 'value')
            advancedVBox = widgets.VBox(children=[displayModeDropdown, exportWidget])
            advancedAccordion = widgets.Accordion(children=[advancedVBox],
                                                  selected_index=None)
            advancedAccordion.set_title(0, 'Advanced...')
            defaultVBox.children += (advancedAccordion,)
            # pandas.options.display.max_rows = pdMaxRows
            # return qgrid.show_grid(data)
        elif isinstance(data, geopy.point.Point):
            pointOnMap = ipyleaflet.Map(
                basemap=ipyleaflet.basemaps.Esri.WorldImagery,
                center=[data[0], data[1]],
                zoom=12,
                layout=widgets.Layout(height='500px' if not inSidecar else maxHeight))
            with out:
                display(pointOnMap)
        elif isinstance(data, PIL.Image.Image):
            with out:
                display(data)
        elif isinstance(data, dict):
            out.append_stdout(pprint.pformat(data))
        elif isinstance(data, str):
            out.append_stdout(data)
        else:
            with out:
                display(data)

        return defaultVBox

    def createArtifactWidgetFor(node, inSidecar=False):
        mfNode = node['mfObj']

        if isinstance(mfNode, DataArtifact):
            return createWidgetForDataArtifact(node, inSidecar=inSidecar)
        elif isinstance(mfNode, Task):
            if node['name'] == 'stdout':
                taskStdOut = mfNode.stdout

                if taskStdOut is None or len(taskStdOut) == 0:
                    taskStdOut = "<stdout empty>"

                artifactWidget = widgets.Output()
                artifactWidget.append_stdout(taskStdOut)
                return artifactWidget
            elif node['name'] == 'stderr':
                taskStdErr = mfNode.stderr

                if taskStdErr is None or len(taskStdErr) == 0:
                    taskStdErr = "<stderr empty>"

                artifactWidget = widgets.Output()
                artifactWidget.append_stdout(taskStdErr)
                return artifactWidget
            elif node['name'] == 'exception':
                if mfNode.exception is not None:
                    artifactWidget = widgets.Output()
                    artifactWidget.append_stdout(str(mfNode.exception))
                return artifactWidget

        return None

    def createTagsWidgetFor(node):
        mfNode = node['mfObj']
        tagsHBox = widgets.HBox()
        tagsHBox.children += (widgets.Label(value="Tags:"),)
        tags = mfNode.tags

        if tags is not None:
            tagsVBox = widgets.VBox()
            tagsHBox.children += (tagsVBox,)
            for tag in tags:
                tagsVBox.children += (widgets.Label(value=tag),)

        return tagsHBox

    _lastClickedStep = None

    class HistoryElButton(widgets.Button):
        def __init__(self, node, widget, *args, **kwargs):
            super(HistoryElButton, self).__init__(*args, **kwargs)
            self.node = node
            self.widget = widget
            self.isSelected = False
            self.icon = node['icon']
            self.button_style = node['icon_style']
            self.originalStyle = self.button_style

            if 'layout' not in args and 'layout' not in kwargs:
                self.layout = widgets.Layout(padding='0px 1px 0px 1px', width='fit-content',
                                             height='fit-content')

        @classmethod
        def clearLastClickedStep(cls) -> None:
            global _lastClickedStep
            if _lastClickedStep is not None:
                _lastClickedStep.click()
                _lastClickedStep = None

        def clearLast(self) -> None:
            global _lastClickedStep

            if _lastClickedStep is not self:
                HistoryElButton.clearLastClickedStep()
                _lastClickedStep = self

        def selectNodeInTree(self) -> None:
            selectTreeNodeFrom(self.node, True)

        def click(self) -> None:
            super().click()
            self.clearLast()
            self.selectNodeInTree()

            if self.isSelected:
                self.button_style = self.originalStyle
            else:
                self.originalStyle = self.button_style
                self.button_style = 'info'

            self.isSelected = not self.isSelected

    def createHistoryWidgetForFlow(flowNode, runNodes=None, maxHistory=32):
        if runNodes is None:
            runNodes = ensureSubNodesFor(flowNode)

        historyVBox = widgets.VBox([pathLabelWidgetFor(flowNode), dividerVArtT])

        if runNodes is None or len(runNodes) == 0:
            historyVBox.children += (
            widgets.Label(value='<No Runs for flow:' + flowNode['name'] + '>'),)
            return historyVBox

        summaryHBox = widgets.HBox()
        runColumnLayout = widgets.Layout(width='200px', min_width='172px')
        maxStepCount = 0
        runWidgets = [widgets.HBox([
            widgets.Label(value='Run:', layout=runColumnLayout),
            widgets.Label(value='Steps:')])]
        runCount = 0

        for run in runNodes:
            if runCount > maxHistory:
                break

            stepNodes = ensureSubNodesFor(run)

            if len(stepNodes) == 0:
                continue

            stepWidgets = []
            historyButton = HistoryElButton(
                node=run,
                widget=summaryHBox,
                description=run['mfObj'].created_at,
                layout=runColumnLayout)
            stepWidgets.append(historyButton)

            for step in stepNodes:
                historyButton = HistoryElButton(
                    node=step,
                    widget=summaryHBox)
                stepWidgets.append(historyButton)

            stepHBox = widgets.HBox(stepWidgets, layout=tightLayout)
            runWidgets.append(stepHBox)
            maxStepCount = max(maxStepCount, len(stepWidgets))
            runCount += 1

        runWidgets.append(dividerVArtB)
        runWidgets.append(summaryHBox)
        historyVBox.children += tuple(runWidgets)
        return historyVBox

    def fullPathSpecFor(node):
        if isFakeNode(node):
            return node['mfObj'].pathspec + '/' + node['name']
        else:
            return node['mfObj'].pathspec

    def canCacheSummaryPaneFor(node):
        if isinstance(node['mfObj'], DataArtifact) and isinstance(node['mfObj'].data,
                                                                  geopy.point.Point):
            return False
        else:
            return True

    def pathLabelWidgetFor(node):
        return widgets.Label(layout=tightLayout,
                             value="{0}: {1}".format(type(node['mfObj']).__name__,
                                                     fullPathSpecFor(node)))

    def createSummaryWidgetFor(node, inSidecar=False):
        mfNode = node['mfObj']
        defaultVBox = widgets.VBox(layout=widgets.Layout(padding='0px',
                                                         width='100%',
                                                         height='fit-content',
                                                         min_height='fit-content'))

        pathLabel = pathLabelWidgetFor(node)
        headerHBox = widgets.HBox([pathLabel])
        defaultVBox.children += (headerHBox,)
        artifactWidget = createArtifactWidgetFor(node, inSidecar=inSidecar)

        if artifactWidget is not None:
            # artifactWidget.layout=defaultLayout
            # artifactWidgetHBox = widgets.HBox(
            # layout=widgets.Layout(width='100%',
            # height='fit-content',
            # min_height='fit-content',
            # border='1px solid lightgray'))
            # artifactWidgetHBox.children += (artifactWidget,)
            # defaultVBox.children += (artifactWidgetHBox,)
            defaultVBox.children += (dividerVArtT, artifactWidget, dividerVArtB)

        if isinstance(mfNode, DataArtifact):
            defaultVBoxOrig = defaultVBox
            detailsAccordionVBox = widgets.VBox(layout=widgets.Layout(padding='0px',
                                                                      width='100%',
                                                                      height='fit-content',
                                                                      min_height='fit-content'))
            detailsAccordion = widgets.Accordion(children=[detailsAccordionVBox],
                                                 titles=('Metadata...',),
                                                 selected_index=None)
            detailsAccordion.set_title(0, 'Metadata...')
            defaultVBoxOrig.children += (detailsAccordion,)
            defaultVBox = detailsAccordionVBox

        tagsWidget = createTagsWidgetFor(node)
        defaultVBox.children += (tagsWidget,)
        defaultVBox.children += (widgets.Label(layout=tightLayout,
                                               value="Created at: {0}".format(
                                                   mfNode.created_at)),)

        if isinstance(mfNode, Task):
            index = mfNode.index
            if index is not None:
                defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                       value="Foreach index: {0}".format(
                                                           mfNode.index)),)
                try:
                    varName = mfNode['_foreach_stack'].data[-1].var
                    defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                           value="Foreach variable: {0}".format(
                                                               varName)),)
                except:
                    pass
        elif isinstance(mfNode, Step):
            defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                   value="Finished at: {0}".format(
                                                       mfNode.finished_at)),)
        elif isinstance(mfNode, Run):
            defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                   value="Finished at: {0}".format(
                                                       mfNode.finished_at)),)
        elif isinstance(mfNode, Flow):
            flowEls = []
            latest_run = mfNode.latest_run
            if latest_run is not None:
                defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                       value="Latest run: {0}".format(
                                                           mfNode.latest_run.path_components[
                                                               -1])),)
            latest_successful_run = mfNode.latest_successful_run
            if latest_successful_run is not None:
                defaultVBox.children += (widgets.Label(layout=tightLayout,
                                                       value="Latest successful run: {0}".format(
                                                           mfNode.latest_successful_run.path_components[
                                                               -1])),)
            historySidecarButton = SidecarButton(
                sidecarObj=lambda: createHistoryWidgetForFlow(node),
                description='History...',
                sidecarTitle="History " + node['name'],
                icon='external-link')
            historySidecarButton.on_click(sidecarButtonClick)
            defaultVBox.children += (historySidecarButton,)

        sidecarButton = SidecarButton(sidecarObj=None,
                                      sidecarTitle=node['name'],
                                      icon='external-link',
                                      layout=tightLayout)
        sidecarButton.on_click(sidecarButtonClick)
        headerHBox.children = (sidecarButton,) + headerHBox.children
        sidecarButton.sidecarObj = lambda: createSummaryWidgetFor(node, True)

        if isinstance(mfNode, DataArtifact):
            return defaultVBoxOrig
        else:
            return defaultVBox

    def shouldAutoRecurse(mfNode):
        if isinstance(mfNode, DataArtifact):
            return False
        elif isinstance(mfNode, Task):
            return False
        elif isinstance(mfNode, Step):
            return False
        elif isinstance(mfNode, Run):
            return False
        elif isinstance(mfNode, Flow):
            return False
        else:
            return False

    def isFakeNode(node):
        return node['isFakeNode']

    class PathElButton(widgets.Button):
        def __init__(self, node, *args, **kwargs):
            super(PathElButton, self).__init__(*args, **kwargs)
            self.node = node

    def selectTreeNodeFrom(node, ensureTreeNodes=False) -> None:
        global selectedNode
        currentlySelectedTreeNode = None

        if selectedNode is not None and selectedNode['treeNode'] is not None:
            currentlySelectedTreeNode = selectedNode['treeNode']

        if ensureTreeNodes:
            currentNode = node
            nodesToSelect = []

            while currentNode is not None:
                nodesToSelect.insert(0, currentNode)
                currentNode = currentNode['parent']

            for nodeToSelect in nodesToSelect:
                selectTreeNode(nodeToSelect['treeNode'], silentSelect=True)

        treeNode = node['treeNode']

        if treeNode is not None and not treeNode.selected:
            if currentlySelectedTreeNode is not None:
                currentlySelectedTreeNode.selected = False

            treeNode.selected = True

    def onPathElClick(button) -> None:
        selectTreeNodeFrom(button.node)

    def updatePathElsFor(node) -> None:
        mfNode = node['mfObj']
        pathWidgets = []
        mfPathElNamesIndex = 0
        layout = widgets.Layout(width='auto')

        parents = []
        nodeParent = node

        while nodeParent is not None:
            parents.insert(0, nodeParent)
            nodeParent = nodeParent['parent']

        for pathEl in mfNode.path_components:
            button = PathElButton(parents[mfPathElNamesIndex],
                                  description=pathEl,
                                  layout=layout,
                                  icon=iconForNodeName(mfPathElNames[mfPathElNamesIndex]))
            button.on_click(onPathElClick)
            pathWidgets.append(button)
            pathWidgets.append(widgets.Button(layout=layout,
                                              disabled=True,
                                              icon='angle-right'))
            mfPathElNamesIndex += 1

        if isFakeNode(node):
            button = PathElButton(node,
                                  description=node['name'],
                                  layout=layout,
                                  icon=iconForNodeName('DataArtifact'))
            button.on_click(onPathElClick)
            pathWidgets.append(button)

        # pathElsWidget.children = tuple(pathWidgets)
        pathWidget.children = tuple(pathWidgets)


    def handleFlowClick(event) -> None:
        selectTreeNode(event['owner'])

    def handleTreeClosed(event) -> None:
        # global _sidecars
        for sidecar in _sidecars:
            sidecar.close()

    def handleFlowOpened(event) -> None:
        mfPathspec = event['owner'].node['mfObj'].pathspec
        openedNodes[mfPathspec] = event['owner'].opened

    def selectTreeNode(treeNode, silentSelect=False) -> None:
        # global selectedNode
        # global selectedNodePathSpec
        flowNode = treeNode.node
        mfNode = flowNode['mfObj']
        selectedNode = flowNode
        selectedNodePathSpec = mfNode.pathspec
        openedNodes[mfNode.pathspec] = True
        flowNode['opened'] = True

        if isinstance(mfNode, Run) and mfNode.successful and not mfNode.finished:
            openedUnfinishedNodes[mfNode.pathspec] = flowNode
            flowNode['watch'] = True

        ensureSubNodesFor(flowNode, shouldAutoRecurse(mfNode))

        if not silentSelect:
            if 'summaryPane' not in flowNode:
                summaryPane = createSummaryWidgetFor(flowNode)

                if summaryPane is not None:
                    if canCacheSummaryPaneFor(flowNode):
                        flowNode['summaryPane'] = summaryPane

                    summaryHBox.children = (summaryPane,)
            else:
                summaryHBox.children = (flowNode['summaryPane'],)

            updatePathElsFor(flowNode)

        if len(treeNode.nodes) > 0:
            return

        subTreeNodes, createdChildren = createTreeNodesFor(nodes=flowNode['nodes'],
                                                           openChildren=True)

        if createdChildren:
            treeNode.nodes = subTreeNodes

    class MFNode(Node):
        def __init__(self, node, *args, **kwargs):
            super(MFNode, self).__init__(*args, **kwargs)
            self.node = node

    def createTreeNodesFor(nodes, recursive=False, nodesToOpen=None, isParentOpen=False,
                           openChildren=False):
        # global selectedNode
        # global selectedNodePathSpec
        subTreeNodes = []
        createdNewTreeNode = False

        if len(nodes) == 0:
            return subTreeNodes, createdNewTreeNode

        for subNode in nodes:
            subSubTreeNodes = []
            mfNode = subNode['mfObj']
            nodePathspec = mfNode.pathspec
            nodeWasOpened = nodesToOpen is not None and nodePathspec in nodesToOpen
            isNodeOpenedNow = nodeWasOpened and nodesToOpen[nodePathspec]

            if (recursive or nodeWasOpened):
                ensureSubNodesFor(subNode)

                if subNode['nodes'] is not None:
                    subSubTreeNodes, createdNewTreeNode = createTreeNodesFor(
                        nodes=subNode['nodes'],
                        recursive=recursive,
                        nodesToOpen=nodesToOpen,
                        isParentOpen=isNodeOpenedNow,
                        openChildren=False)

            isSelectedNode = selectedNodePathSpec == subNode[
                'mfObj'].pathspec and not isFakeNode(subNode)
            subTreeNode = subNode['treeNode'] if 'treeNode' in subNode else None

            if subTreeNode is None:
                createdNewTreeNode = True
                subTreeNode = MFNode(subNode,
                                     subNode['name'],
                                     subSubTreeNodes,
                                     icon=subNode['icon'],
                                     icon_style=subNode['icon_style'],
                                     opened=isNodeOpenedNow or openChildren,
                                     selected=isSelectedNode)
                subNode['treeNode'] = subTreeNode

            if isSelectedNode:
                selectedNode = subNode

            subTreeNode.observe(handleFlowClick, 'selected')
            subTreeNode.observe(handleFlowOpened, 'opened')
            subTreeNodes.append(subTreeNode)

        return subTreeNodes, createdNewTreeNode

    def refreshTreeClick(button) -> None:
        # global openedNodes
        # global selectedNode
        summaryHBox.children = tuple()
        flowVNodes = createNodesFor(Metaflow())
        tree.nodes, _ = createTreeNodesFor(nodes=flowVNodes, nodesToOpen=openedNodes)
        if len(openedNodes) > 0:
            selectTreeNode(selectedNode['treeNode'])

    flowVNodes = createNodesFor(Metaflow())
    treeNodes, _ = createTreeNodesFor(nodes=flowVNodes, openChildren=True)
    selectedNode = flowVNodes[0]
    tree = Tree(nodes=treeNodes,
                multiple_selection=False,
                animation=0,
                layout=widgets.Layout(
                    width='33%', max_width='33%',
                    min_height='200px', max_height=maxHeight,
                    overflow_x='auto', overflow_y='auto'))
    tree.observe(handleTreeClosed, 'closed')
    hbox.children = (tree, divider, summaryHBox)
    vbox = widgets.VBox()
    refreshButton = widgets.Button(icon='refresh', layout=tightLayout)
    refreshButton.on_click(refreshTreeClick)
    titleWidget = widgets.HBox(children=[refreshButton, pathWidget],
                               layout=tightLayout)
    vbox.children = (titleWidget, dividerV, hbox)
    return vbox

