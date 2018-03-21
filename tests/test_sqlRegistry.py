# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import unittest

import lsst.utils.tests

from lsst.daf.butler.core.storageInfo import StorageInfo
from lsst.daf.butler.core.run import Run
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.registry import Registry
from lsst.daf.butler.registries.sqlRegistry import SqlRegistry

"""Tests for SqlRegistry.
"""


class SqlRegistryTestCase(lsst.utils.tests.TestCase):
    """Test for SqlRegistry.
    """

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")

    def testInitFromConfig(self):
        registry = Registry.fromConfig(self.configFile)
        self.assertIsInstance(registry, SqlRegistry)

    def testDatasetType(self):
        registry = Registry.fromConfig(self.configFile)
        # Check valid insert
        datasetTypeName = "test"
        storageClass = "StructuredData"
        dataUnits = ("camera", "visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

        # Re-inserting should fail
        with self.assertRaises(KeyError):
            registry.registerDatasetType(inDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = "StructuredData"
        dataUnits = ("camera", "visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

    def testComponents(self):
        registry = Registry.fromConfig(self.configFile)
        parentDatasetType = DatasetType(name="parent", dataUnits=("camera",), storageClass="dummy")
        childDatasetType = DatasetType(name="child", dataUnits=("camera",), storageClass="dummy")
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType)
        run = registry.makeRun(collection="test")
        parent = registry.addDataset(parentDatasetType, dataId={"camera": "DummyCam"}, run=run)
        children = {"child1": registry.addDataset(childDatasetType, dataId={"camera": "DummyCam"}, run=run),
                    "child2": registry.addDataset(childDatasetType, dataId={"camera": "DummyCam"}, run=run)}
        for name, child in children.items():
            registry.attachComponent(name, parent, child)
        self.assertEqual(parent.components, children)

    def testRun(self):
        registry = Registry.fromConfig(self.configFile)
        # Check insertion and retrieval with two different collections
        for collection in ["one", "two"]:
            run = registry.makeRun(collection)
            self.assertIsInstance(run, Run)
            self.assertEqual(run.collection, collection)
            # Test retrieval by collection
            runCpy1 = registry.getRun(collection=run.collection)
            self.assertEqual(runCpy1, run)
            # Test retrieval by (run/execution) id
            runCpy2 = registry.getRun(id=run.execution)
            self.assertEqual(runCpy2, run)
        # Non-existing collection should return None
        self.assertIsNone(registry.getRun(collection="bogus"))
        # Non-existing id should return None
        self.assertIsNone(registry.getRun(id=100))

    def testStorageInfo(self):
        registry = Registry.fromConfig(self.configFile)
        datasetType = DatasetType(name="test", dataUnits=("camera",), storageClass="dummy")
        registry.registerDatasetType(datasetType)
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        datastoreName = "dummystore"
        checksum = "d6fb1c0c8f338044b2faaf328f91f707"
        size = 512
        storageInfo = StorageInfo(datastoreName, checksum, size)
        # Test adding information about a new dataset
        registry.addStorageInfo(ref, storageInfo)
        outStorageInfo = registry.getStorageInfo(ref, datastoreName)
        self.assertEqual(outStorageInfo, storageInfo)
        # Test updating storage information for an existing dataset
        updatedStorageInfo = StorageInfo(datastoreName, "20a38163c50f4aa3aa0f4047674f8ca7", size+1)
        registry.updateStorageInfo(ref, datastoreName, updatedStorageInfo)
        outStorageInfo = registry.getStorageInfo(ref, datastoreName)
        self.assertNotEqual(outStorageInfo, storageInfo)
        self.assertEqual(outStorageInfo, updatedStorageInfo)

    def testAssembler(self):
        registry = Registry.fromConfig(self.configFile)
        datasetType = DatasetType(name="test", dataUnits=("camera",), storageClass="dummy")
        registry.registerDatasetType(datasetType)
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        self.assertIsNone(ref.assembler)
        assembler = "some.fully.qualified.assembler"  # TODO replace by actual dummy assember once implemented
        registry.setAssembler(ref, assembler)
        self.assertEqual(ref.assembler, assembler)
        # TODO add check that ref2.assembler is also correct when ref2 is returned by Registry.find()

    def testFind(self):
        registry = Registry.fromConfig(self.configFile)
        datasetType = DatasetType(name="dummytype", dataUnits=("camera", "visit"), storageClass="dummy")
        registry.registerDatasetType(datasetType)
        collection = "test"
        dataId = {"camera": "DummyCam", "visit": 0}
        run = registry.makeRun(collection=collection)
        inputRef = registry.addDataset(datasetType, dataId=dataId, run=run)
        outputRef = registry.find(collection, datasetType, dataId)
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(ValueError):
            dataId = {"camera": "DummyCam", "abstract_filter": "g"}  # should be visit
            registry.find(collection, datasetType, dataId)
        # Check that different dataIds match to different datasets
        dataId1 = {"camera": "DummyCam", "visit": 1}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"camera": "DummyCam", "visit": 2}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        dataId3 = {"camera": "MyCam", "visit": 2}
        inputRef3 = registry.addDataset(datasetType, dataId=dataId3, run=run)
        self.assertEqual(registry.find(collection, datasetType, dataId1), inputRef1)
        self.assertEqual(registry.find(collection, datasetType, dataId2), inputRef2)
        self.assertEqual(registry.find(collection, datasetType, dataId3), inputRef3)
        self.assertNotEqual(registry.find(collection, datasetType, dataId1), inputRef2)
        self.assertNotEqual(registry.find(collection, datasetType, dataId2), inputRef1)
        self.assertNotEqual(registry.find(collection, datasetType, dataId3), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"camera": "DummyCam", "visit": 42}
        self.assertIsNone(registry.find(collection, datasetType, nonExistingDataId))

    def testCollections(self):
        registry = Registry.fromConfig(self.configFile)
        datasetType = DatasetType(name="dummytype", dataUnits=("camera", "visit"), storageClass="dummy")
        registry.registerDatasetType(datasetType)
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        dataId1 = {"camera": "DummyCam", "visit": 0}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"camera": "DummyCam", "visit": 1}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        # We should be able to find both datasets in their Run.collection
        outputRef = registry.find(run.collection, datasetType, dataId1)
        self.assertEqual(outputRef, inputRef1)
        outputRef = registry.find(run.collection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)
        # and with the associated collection
        newCollection = "something"
        registry.associate(newCollection, [inputRef1, inputRef2])
        outputRef = registry.find(newCollection, datasetType, dataId1)
        self.assertEqual(outputRef, inputRef1)
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)
        # but no more after disassociation
        registry.disassociate(newCollection, [inputRef1, ], remove=False)  # TODO test with removal when done
        self.assertIsNone(registry.find(newCollection, datasetType, dataId1))
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
