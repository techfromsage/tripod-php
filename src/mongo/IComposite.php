<?php

declare(strict_types=1);

namespace Tripod\Mongo\Composites;

use Tripod\Mongo\ImpactedSubject;

interface IComposite
{
    /**
     * Returns the operation this composite can satisfy.
     */
    public function getOperationType(): string;

    /**
     * Returns the subjects that this composite will need to regenerate given changes made to the underlying dataset.
     *
     * @return ImpactedSubject[]
     */
    public function getImpactedSubjects(array $subjectsAndPredicatesOfChange, string $contextAlias): array;

    /**
     * Invalidate/regenerate the composite based on the impacted subject.
     */
    public function update(ImpactedSubject $subject): void;
}
