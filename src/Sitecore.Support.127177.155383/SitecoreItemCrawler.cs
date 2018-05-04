using Sitecore.Data.LanguageFallback;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using System;
using System.Linq;
using System.Threading;
using Sitecore.Data;
using Sitecore.Data.Managers;
using Sitecore.Globalization;
using Sitecore.SecurityModel;

namespace Sitecore.Support.ContentSearch
{
    public class SitecoreItemCrawler : Sitecore.ContentSearch.SitecoreItemCrawler
    {
        protected override void DoUpdate(IProviderUpdateContext context, SitecoreIndexableItem indexable, IndexEntryOperationContext operationContext)
        {
            Assert.ArgumentNotNull(context, "context");
            Assert.ArgumentNotNull(indexable, "indexable");
            using (new LanguageFallbackItemSwitcher(new bool?(base.Index.EnableItemLanguageFallback)))
            {
                if (this.IndexUpdateNeedDelete(indexable))
                {
                    object[] parameters = new object[] { base.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                    base.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:deleteitem", parameters);
                    base.Operations.Delete(indexable, context);
                }
                else
                {
                    object[] objArray2 = new object[] { base.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                    base.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatingitem", objArray2);
                    if (!this.IsExcludedFromIndex(indexable, true))
                    {
                        if ((operationContext != null) && !operationContext.NeedUpdateAllVersions)
                        {
                            this.UpdateItemVersion(context, (Item)indexable, operationContext);
                        }
                        else
                        {
                            foreach (Language language in ((operationContext != null) && !operationContext.NeedUpdateAllLanguages) ? new Language[] { indexable.Item.Language } : indexable.Item.Languages)
                            {
                                Item item;
                                using (new WriteCachesDisabler())
                                {
                                    using (new SecurityDisabler())
                                    {
                                        item = indexable.Item.Database.GetItem(indexable.Item.ID, language, Data.Version.Latest);
                                    }
                                }
                                if (item == null)
                                {
                                    CrawlingLog.Log.Warn(string.Format("SitecoreItemCrawler : Update : Latest version not found for item {0}. Skipping.", indexable.Item.Uri), null);
                                }
                                else
                                {
                                    Item[] itemArray;
                                    // Sitecore.Support.155383. Switched from SitecoreCachesDisabler() to WriteCachesDisabler()
                                    using (new WriteCachesDisabler())
                                    {
                                        itemArray = !item.IsFallback ? item.Versions.GetVersions(false) : new Item[] { item };
                                    }
                                    foreach (Item item2 in itemArray)
                                    {
                                        this.UpdateItemVersion(context, item2, operationContext);
                                    }
                                }
                            }
                        }
                        object[] objArray3 = new object[] { base.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                        base.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updateditem", objArray3);
                    }
                    if (base.DocumentOptions.ProcessDependencies)
                    {
                        object[] objArray4 = new object[] { base.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                        base.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatedependents", objArray4);
                        this.UpdateDependents(context, indexable);
                    }
                }
            }
        }

        // Sitecore.Support.127177. LanguageFallback context was added to take the index language fallback configuration into account.
        protected override SitecoreIndexableItem GetIndexableAndCheckDeletes(IIndexableUniqueId indexableUniqueId)
        {
            using (new LanguageFallbackItemSwitcher(new bool?(base.index.EnableItemLanguageFallback)))
            {
                ItemUri itemUri = (ItemUri)(indexableUniqueId as SitecoreItemUniqueId);
                using (new SecurityDisabler())
                {
                    Item item;
                    WriteCachesDisabler disabler2;
                    using (disabler2 = new WriteCachesDisabler())
                    {
                        item = this.GetItem(itemUri);
                    }
                    if (item != null)
                    {
                        Data.Version[] versionArray;
                        ItemUri uri = new ItemUri(itemUri.ItemID, itemUri.Language, Data.Version.Latest, itemUri.DatabaseName);
                        Item item2 = this.GetItem(uri);
                        using (disabler2 = new WriteCachesDisabler())
                        {
                            versionArray = item2.Versions.GetVersionNumbers() ?? new Data.Version[0];
                        }
                        if (itemUri.Version != Data.Version.Latest && versionArray.All(v => v.Number != itemUri.Version.Number))
                        {
                            item = null;
                        }
                    }
                    return item;
                }
            }
        }

        public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexEntryOperationContext operationContext, IndexingOptions indexingOptions = 0)
        {
            Assert.ArgumentNotNull(indexableUniqueId, "indexableUniqueId");
            if (this.CircularReferencesIndexingGuard.TryAddToProcessedList(indexableUniqueId, this, context) && base.ShouldStartIndexing(indexingOptions))
            {
                Assert.IsNotNull(base.DocumentOptions, "DocumentOptions");
                if (!this.IsExcludedFromIndex(indexableUniqueId, operationContext, true))
                {
                    if (operationContext != null)
                    {
                        if (operationContext.NeedUpdateChildren)
                        {
                            SitecoreIndexableItem indexable = this.GetIndexable(indexableUniqueId as SitecoreItemUniqueId);
                            if (indexable != null)
                            {
                                if (((operationContext.OldParentId != Guid.Empty) && this.IsRootOrDescendant(new ID(operationContext.OldParentId))) && !this.IsAncestorOf((Item)indexable))
                                {
                                    this.Delete(context, indexableUniqueId, IndexingOptions.Default);
                                    return;
                                }
                                this.UpdateHierarchicalRecursive(context, indexable, CancellationToken.None);
                                return;
                            }
                        }
                        if (operationContext.NeedUpdatePreviousVersion)
                        {
                            SitecoreIndexableItem item3 = this.GetIndexable(indexableUniqueId as SitecoreItemUniqueId);
                            if (item3 != null)
                            {
                                this.UpdatePreviousVersion((Item)item3, context);
                            }
                        }
                        if (operationContext.NeedUpdateAllVersions)
                        {
                            Item item = this.GetItem((ItemUri)(indexableUniqueId as SitecoreItemUniqueId));
                            if (item != null)
                            {
                                this.DoUpdate(context, item, operationContext);
                                return;
                            }
                        }
                    }
                    SitecoreIndexableItem indexableAndCheckDeletes = this.GetIndexableAndCheckDeletes(indexableUniqueId);
                    if (indexableAndCheckDeletes == null)
                    {
                        if (this.GroupShouldBeDeleted(indexableUniqueId.GroupId))
                        {
                            this.Delete(context, indexableUniqueId.GroupId, IndexingOptions.Default);
                        }
                        else
                        {
                            this.Delete(context, indexableUniqueId, IndexingOptions.Default);
                        }
                    }
                    else
                    {
                        this.DoUpdate(context, indexableAndCheckDeletes, operationContext);
                    }
                }
            }
        }

        private bool IsRootOrDescendant(ID id)
        {
            Item item;
            if (this.RootItem.ID == id)
            {
                return true;
            }
            using (new SecurityDisabler())
            {
                item = this.GetItem(id);
            }
            return ((item != null) && this.IsAncestorOf(item));
        }

        private void UpdatePreviousVersion(Item item, IProviderUpdateContext context)
        {
            Data.Version[] versionArray;
            Data.Version previousVersion;
            using (new WriteCachesDisabler())
            {
                versionArray = item.Versions.GetVersionNumbers() ?? new Data.Version[0];
            }
            int num = versionArray.ToList().FindIndex(version => version.Number == item.Version.Number);
            if (num >= 1)
            {
                previousVersion = versionArray[num - 1];
                Data.Version version = versionArray.FirstOrDefault(ver => ver == previousVersion);
                SitecoreIndexableItem indexable = Sitecore.Data.Database.GetItem(new ItemUri(item.ID, item.Language, version, item.Database.Name));
                if (indexable != null)
                {
                    ((IIndexableBuiltinFields)indexable).IsLatestVersion = false;
                    indexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;
                    base.Operations.Update(indexable, context, base.index.Configuration);
                }
            }
        }

        public virtual void Delete(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            if (!ShouldStartIndexing(indexingOptions))
                return;

            context.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:deleteitem", this.index.Name, indexableUniqueId);
            this.Operations.Delete(indexableUniqueId, context);

            var deleteDependencies = this.GetIndexablesToUpdateOnDelete(indexableUniqueId);
            var deleteDependencyIndexables = deleteDependencies.Select(this.GetIndexable).Where(i => i != null);

            IndexEntryOperationContext operationContext = new IndexEntryOperationContext
            {
                NeedUpdateAllLanguages = false,
                NeedUpdateAllVersions = false,
                NeedUpdateChildren = false
            };

            // Sitecore.Support.127177. Takes into account LanguageFallback configuration of the index.
            if (index.EnableItemLanguageFallback)
            {
                this.DoUpdateFallbackField(context, indexableUniqueId);
                foreach (var deleteDependencyIndexable in deleteDependencyIndexables)
                {
                    this.DoUpdate(context, deleteDependencyIndexable, operationContext);
                }
            }
        }

        private void DoUpdateFallbackField(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId)
        {
            var indexableItem = this.GetIndexable(indexableUniqueId) as SitecoreIndexableItem;
            if (indexableItem == null)
            {
                return;
            }
            indexableItem.Item.Fields.ReadAll();
            var fields = indexableItem.Item.Fields;
            if (fields.Any(e => e.SharedLanguageFallbackEnabled))
            {
                var fallbackItems = LanguageFallbackManager.GetDependentLanguages(indexableItem.Item.Language, indexableItem.Item.Database, indexableItem.Item.ID)
                  .SelectMany(
                    language =>
                    {
                        var item1 = indexableItem.Item.Database.GetItem(indexableItem.Item.ID, language);
                        return item1 != null ? item1.Versions.GetVersions() : new Item[0];
                    });

                foreach (var fallbackItem in fallbackItems)
                {
                    var valueToConsider = new SitecoreItemUniqueId(fallbackItem.Uri);
                    this.Update(context, valueToConsider);
                }
            }
        }
    }
}