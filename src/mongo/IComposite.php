<?php
interface IComposite
{
    /**
     * Returns the operation this composite can satisfy
     * @return string
     */
    public function getOperationType();

    /**
     * Returns the subjects that this composite will need to regenerate given changes made to the underlying dataset
     * @param ChangeSet $cs
     * @param string $contextAlias
     * @return mixed
     */
    public function getImpactedSubjects(ChangeSet $cs,$contextAlias);

    public function update(ImpactedSubject $subject);
}
