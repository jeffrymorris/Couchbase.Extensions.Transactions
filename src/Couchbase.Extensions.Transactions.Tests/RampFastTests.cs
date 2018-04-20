using System;
using System.Collections.Generic;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.IO;
using NUnit.Framework;

namespace Couchbase.Extensions.Transactions.Tests
{
    public class RampFastTests
    {
        private ICluster _cluster;
        private IBucket _bucket;

        [SetUp]
        public void SetUp()
        {
            _cluster = new Cluster(new ClientConfiguration
            {
                Servers = new List<Uri>
                {
                    new Uri("http://10.142.180.102:8091/")
                }
            });

            _cluster.Authenticate("Administrator", "password");
            _bucket = _cluster.OpenBucket();

            if (_bucket.Exists("a"))
            {
                var a = _bucket.Remove("a");
                Assert.IsTrue(a.Success);
            }

            if (_bucket.Exists("b"))
            {
                var b = _bucket.Remove("b");
                Assert.IsTrue(b.Success);
            }

            if (_bucket.Exists("c"))
            {
                var c = _bucket.Remove("c");
                Assert.IsTrue(c.Success);
            }

            if (!_bucket.Exists("counter"))
            {
                var counter = _bucket.Increment("counter");
                Assert.IsTrue(counter.Success);
            }

            var insertC = _bucket.Insert("c", new { });
            Assert.AreEqual(ResponseStatus.Success, insertC.Status);
        }

        [Test]
        public void PrepareTest()
        {
            var a = _bucket.Get<dynamic>("a");
            var b = _bucket.Get<dynamic>("b");
            var c = _bucket.Exists("c");

            Assert.AreEqual(ResponseStatus.KeyNotFound, a.Status);
            Assert.AreEqual(ResponseStatus.KeyNotFound, b.Status);
            Assert.IsTrue(c);

            a = _bucket.Insert("a", new { });
            Assert.IsTrue(a.Success);

            var increment = _bucket.Increment("counter");
            Assert.IsTrue(increment.Success);

            var aMeta = _bucket.MutateIn<dynamic>("a").
                Insert("tx",  new
                {
                    ts=increment.Value,
                    md=new[]{b.Id},
                    value=new {name="jeff"}
                }, SubdocPathFlags.Xattr);

            var execute = aMeta.Execute();
            Assert.IsTrue(execute.Success);

            a = _bucket.Get<dynamic>("a");
            Assert.AreEqual("", a.Value);

            b = _bucket.Insert("b", new { });
            Assert.IsTrue(b.Success);

            var bMeta = _bucket.MutateIn<dynamic>("b").
                Insert("tx",  new
                {
                    ts=increment.Value,
                    md=new[]{a.Id},
                    value=new {name="mike"}
                }, SubdocPathFlags.Xattr);

            execute = bMeta.Execute();
            Assert.IsTrue(execute.Success);

            b = _bucket.Get<dynamic>("b");
            Assert.AreEqual("", b.Value);

            dynamic aTx = _bucket.LookupIn<dynamic>("a").Get("tx", SubdocPathFlags.Xattr).Execute().Content("tx");
            dynamic bTx = _bucket.LookupIn<dynamic>("b").Get("tx", SubdocPathFlags.Xattr).Execute().Content("tx");

            //ts
            Assert.AreEqual(increment.Value, aTx.ts.Value);
            Assert.AreEqual(increment.Value, bTx.ts.Value);

            //md
            Assert.AreEqual(a.Id, bTx.md[0].Value);
            Assert.AreEqual(b.Id, aTx.md[0].Value);

            //value
            Assert.AreEqual("jeff", aTx.value.name.Value);
            Assert.AreEqual("mike", bTx.value.name.Value);
        }

        //prepare two inserts and committing scenario
        [Test]
        public void CommitTest()
        {
            PrepareTest();

            //swap a
            var a = _bucket.Get<dynamic>("a");
            Assert.IsTrue(a.Success);

            dynamic aTx = _bucket.LookupIn<dynamic>("a").Get("tx", SubdocPathFlags.Xattr).Execute().Content("tx");

            a = _bucket.Replace("a", aTx.value.name.Value);
            Assert.IsTrue(a.Success);

            var remove = _bucket.MutateIn<dynamic>("a").Remove("tx", SubdocPathFlags.Xattr).Execute();
            Assert.IsTrue(remove.Success);

            //swap b
            var b = _bucket.Get<dynamic>("b");
            Assert.IsTrue(b.Success);

            dynamic bTx = _bucket.LookupIn<dynamic>("b").Get("tx", SubdocPathFlags.Xattr).Execute().Content("tx");

            b = _bucket.Replace("b", bTx.value.name.Value);
            Assert.IsTrue(b.Success);

            remove = _bucket.MutateIn<dynamic>("b").Remove("tx", SubdocPathFlags.Xattr).Execute();
            Assert.IsTrue(remove.Success);

            //validate a
            a = _bucket.Get<dynamic>("a");
            Assert.IsTrue(a.Success);
            Assert.AreEqual("jeff", a.Value);

            var aTxExists = _bucket.LookupIn<dynamic>("a").Get("tx", SubdocPathFlags.Xattr).Execute();
            Assert.IsFalse(aTxExists.Success);

            //validate b
            b = _bucket.Get<dynamic>("b");
            Assert.IsTrue(b.Success);
            Assert.AreEqual("mike", b.Value);

            var bTxExists = _bucket.LookupIn<dynamic>("b").Get("tx", SubdocPathFlags.Xattr).Execute();
            Assert.IsFalse(bTxExists.Success);
        }


        //prepare to replaces with committing scenario
        [Test]
        public void Transaction_With_Replace()
        {
            CommitTest();

            var a = _bucket.Get<dynamic>("a");
            var b = _bucket.Get<dynamic>("b");

            Assert.IsTrue(a.Success);
            Assert.IsTrue(b.Success);

        }

        [TearDown]
        public void TearDown()
        {
            _bucket.Dispose();
            _cluster.Dispose();
        }
    }
}
